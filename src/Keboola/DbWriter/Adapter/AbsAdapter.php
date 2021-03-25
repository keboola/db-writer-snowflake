<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Adapter;

use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Writer\Snowflake;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use MicrosoftAzure\Storage\Common\Internal\Resources;

class AbsAdapter implements IAdapter
{
    private bool $isSliced;

    private string $region;

    private string $container;

    private string $name;

    private string $connectionEndpoint;

    private string $connectionAccessSignature;

    private string $expiration;

    public function __construct(array $absInfo)
    {
        preg_match(
            '/BlobEndpoint=https?:\/\/(.+);SharedAccessSignature=(.+)/',
            $absInfo['credentials']['sas_connection_string'],
            $connectionInfo
        );
        $this->isSliced = $absInfo['is_sliced'];
        $this->region = $absInfo['region'];
        $this->container = $absInfo['container'];
        $this->name = $absInfo['name'];
        $this->connectionEndpoint = $connectionInfo[1];
        $this->connectionAccessSignature = $connectionInfo[2];
        $this->expiration = $absInfo['credentials']['expiration'];
    }

    public function generateCreateStageCommand(string $stageName): string
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', Snowflake::quote(','));
        $csvOptions[] = sprintf('FIELD_OPTIONALLY_ENCLOSED_BY = %s', Snowflake::quote('"'));
        $csvOptions[] = sprintf('ESCAPE_UNENCLOSED_FIELD = %s', Snowflake::quote('\\'));

        if (!$this->isSliced) {
            $csvOptions[] = 'SKIP_HEADER = 1';
        }

        return sprintf(
            "CREATE OR REPLACE STAGE %s
             FILE_FORMAT = (TYPE=CSV %s)
             URL = 'azure://%s/%s'
             CREDENTIALS = (AZURE_SAS_TOKEN = %s)
            ",
            Snowflake::quoteIdentifier($stageName),
            implode(' ', $csvOptions),
            $this->connectionEndpoint,
            $this->container,
            Snowflake::quote($this->connectionAccessSignature)
        );
    }

    public function generateCopyCommands(string $tableName, string $stageName, array $columns): iterable
    {
        $filesToImport = $this->getManifestEntries();
        foreach (array_chunk($filesToImport, self::SLICED_FILES_CHUNK_SIZE) as $files) {
            $quotedFiles = array_map(
                function ($entry) {
                    return Snowflake::quote(
                        strtr($entry, [$this->getContainerUrl() . '/' => ''])
                    );
                },
                $files
            );

            yield sprintf(
                'COPY INTO %s(%s) 
            FROM (SELECT %s FROM %s t)
            FILES = (%s)',
                $tableName,
                implode(', ', SqlHelper::getQuotedColumnsNames($columns)),
                implode(', ', SqlHelper::getColumnsTransformation($columns)),
                Snowflake::quote('@' . Snowflake::quoteIdentifier($stageName) . '/'),
                implode(',', $quotedFiles)
            );
        }
    }

    private function getManifestEntries(): array
    {
        $blobClient = $this->getClient();
        if (!$this->isSliced) {
            // this is temporary solution copy into is not failing when blob not exists
            try {
                $blobClient->getBlob($this->container, $this->name);
            } catch (ServiceException $e) {
                throw new UserException('Load error: ' . $e->getErrorText(), 0, $e);
            }

            [$this->getContainerUrl() . $this->name];
        }

        try {
            $manifestBlob = $blobClient->getBlob($this->container, $this->name);
        } catch (ServiceException $e) {
            throw new UserException('Load error: manifest file was not found.', 0, $e);
        }

        $manifest = \GuzzleHttp\json_decode((string) stream_get_contents($manifestBlob->getContentStream()), true);
        return array_map(function (array $entry) use ($blobClient) {
            // this is temporary solution copy into is not failing when blob not exists
            try {
                /** @var string[] $parts */
                $parts = explode(sprintf('blob.core.windows.net/%s/', $this->container), $entry['url']);
                $blobPath = $parts[1];
                $blobClient->getBlob($this->container, $blobPath);
            } catch (ServiceException $e) {
                throw new UserException('Load error: ' . $e->getErrorText(), 0, $e);
            }
            return str_replace('azure://', 'https://', $entry['url']);
        }, $manifest['entries']);
    }

    private function getContainerUrl(): string
    {
        return sprintf('https://%s/%s', $this->connectionEndpoint, $this->container);
    }

    private function getClient(): BlobRestProxy
    {
        $sasConnectionString = sprintf(
            '%s=https://%s;%s=%s',
            Resources::BLOB_ENDPOINT_NAME,
            $this->connectionEndpoint,
            Resources::SAS_TOKEN_NAME,
            $this->connectionAccessSignature
        );

        return BlobRestProxy::createBlobService($sasConnectionString);
    }
}
