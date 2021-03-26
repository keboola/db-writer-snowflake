<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Adapter;

use Aws\Exception\AwsException;
use Aws\S3\S3Client;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Writer\Snowflake;

class S3Adapter implements IAdapter
{
    private bool $isSliced;

    private string $region;

    private string $bucket;

    private string $key;

    private string $accessKeyId;

    private string $secretAccessKey;

    private string $sessionToken;

    public function __construct(array $s3info)
    {
        $this->isSliced = $s3info['isSliced'];
        $this->region = $s3info['region'];
        $this->bucket = $s3info['bucket'];
        $this->key = $s3info['key'];
        $this->accessKeyId = $s3info['credentials']['access_key_id'];
        $this->secretAccessKey = $s3info['credentials']['secret_access_key'];
        $this->sessionToken = $s3info['credentials']['session_token'];
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
             URL = 's3://%s'
             CREDENTIALS = (AWS_KEY_ID = %s AWS_SECRET_KEY = %s  AWS_TOKEN = %s)
            ",
            Snowflake::quoteIdentifier($stageName),
            implode(' ', $csvOptions),
            $this->bucket,
            Snowflake::quote($this->accessKeyId),
            Snowflake::quote($this->secretAccessKey),
            Snowflake::quote($this->sessionToken)
        );
    }

    public function generateCopyCommands(string $tableName, string $stageName, array $columns): iterable
    {
        $filesToImport = $this->getManifestEntries();
        foreach (array_chunk($filesToImport, self::SLICED_FILES_CHUNK_SIZE) as $files) {
            $quotedFiles = array_map(
                function ($entry) {
                    return Snowflake::quote(
                        strtr($entry, [$this->getS3Prefix() . '/' => ''])
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
        if (!$this->isSliced) {
            return [$this->getS3Prefix() . '/' . $this->key];
        }

        $client = $this->getClient();
        try {
            $response = $client->getObject([
                'Bucket' => $this->bucket,
                'Key' => ltrim($this->key, '/'),
            ]);
        } catch (AwsException $e) {
            throw new UserException('Load error: ' . $e->getMessage(), $e->getCode(), $e);
        }

        $manifest = json_decode((string) $response['Body'], true);
        return array_map(static function ($entry) {
            return $entry['url'];
        }, $manifest['entries']);
    }

    private function getS3Prefix(): string
    {
        return sprintf('s3://%s', $this->bucket);
    }

    private function getClient(): S3Client
    {
        return new S3Client([
            'credentials' => [
                'key' => $this->accessKeyId,
                'secret' => $this->secretAccessKey,
                'token' => $this->sessionToken,
            ],
            'region' => $this->region,
            'version' => '2006-03-01',
        ]);
    }
}
