<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer\Strategy;

use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Writer\QuoteTrait;
use Keboola\FileStorage\Abs\RetryMiddlewareFactory;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;
use MicrosoftAzure\Storage\Common\Exceptions\ServiceException;
use MicrosoftAzure\Storage\Common\Internal\Resources;

class AbsWriteStrategy implements WriteStrategy
{
    use QuoteTrait;

    private bool $isSliced;

    private string $container;

    private string $name;

    private string $connectionEndpoint;

    private string $connectionAccessSignature;

    public function __construct(array $absInfo)
    {
        preg_match(
            '/BlobEndpoint=https?:\/\/(.+);SharedAccessSignature=(.+)/',
            $absInfo['credentials']['sas_connection_string'],
            $connectionInfo,
        );
        $this->isSliced = $absInfo['is_sliced'];
        $this->container = $absInfo['container'];
        $this->name = $absInfo['name'];
        $this->connectionEndpoint = $connectionInfo[1];
        $this->connectionAccessSignature = $connectionInfo[2];
    }

    public function generateCreateStageCommand(string $stageName): string
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', $this->quote(','));
        $csvOptions[] = sprintf('FIELD_OPTIONALLY_ENCLOSED_BY = %s', $this->quote('"'));
        $csvOptions[] = sprintf('ESCAPE_UNENCLOSED_FIELD = %s', $this->quote('\\'));

        if (!$this->isSliced) {
            $csvOptions[] = 'SKIP_HEADER = 1';
        }

        return sprintf(
            "CREATE OR REPLACE STAGE %s
             FILE_FORMAT = (TYPE=CSV %s)
             URL = 'azure://%s/%s'
             CREDENTIALS = (AZURE_SAS_TOKEN = %s)
            ",
            $this->quoteIdentifier($stageName),
            implode(' ', $csvOptions),
            $this->connectionEndpoint,
            $this->container,
            $this->quote($this->connectionAccessSignature),
        );
    }

    public function generateCopyCommands(string $tableName, string $stageName, array $items): iterable
    {
        $filesToImport = $this->getManifestEntries();
        foreach (array_chunk($filesToImport, self::SLICED_FILES_CHUNK_SIZE) as $files) {
            $quotedFiles = array_map(
                fn($entry) => $this->quote(strtr($entry, [$this->getContainerUrl() . '/' => ''])),
                $files,
            );

            yield sprintf(
                'COPY INTO %s(%s) FROM (SELECT %s FROM %s t) FILES = (%s)',
                $tableName,
                implode(', ', SqlHelper::getQuotedColumnsNames($items)),
                implode(', ', SqlHelper::getColumnsTransformation($items)),
                $this->quote('@' . $this->quoteIdentifier($stageName) . '/'),
                implode(',', $quotedFiles),
            );
        }
    }

    private function getManifestEntries(): array
    {
        $blobClient = $this->getClient();
        if (!$this->isSliced) {
             return [$this->getContainerUrl() . $this->name];
        }
        try {
            $manifestBlob = $blobClient->getBlob($this->container, $this->name);
        } catch (ServiceException $e) {
            throw new UserException('Load error: manifest file was not found.', 0, $e);
        }

        /** @var array{'entries': array{'url': string}[]} $manifest */
        $manifest = json_decode((string) stream_get_contents($manifestBlob->getContentStream()), true);
        return array_map(function (array $entry) {
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
            $this->connectionAccessSignature,
        );

        $blobRestProxy = BlobRestProxy::createBlobService($sasConnectionString);
        $blobRestProxy->pushMiddleware(RetryMiddlewareFactory::create());

        return $blobRestProxy;
    }
}
