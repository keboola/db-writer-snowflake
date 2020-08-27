<?php

namespace Keboola\DbWriter\Snowflake\Test;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\GetFileOptions;

class StagingStorageLoader
{
    public const STORAGE_ABS = 'abs';

    public const STORAGE_S3 = 's3';

    private string $dataDir;

    private Client $storageApi;

    public function __construct(string $dataDir, Client $storageApiClient)
    {
        $this->dataDir = $dataDir;
        $this->storageApi = $storageApiClient;
    }

    private function getInputCsv(string $tableId): string
    {
        return sprintf($this->dataDir . '/in/tables/%s.csv', $tableId);
    }

    public function upload(string $tableId): array
    {
        $filePath = $this->getInputCsv($tableId);
        $bucketId = 'in.c-test-wr-db-snowflake';
        if (!$this->storageApi->bucketExists($bucketId)) {
            $this->storageApi->createBucket('test-wr-db-snowflake', Client::STAGE_IN, '', 'snowflake');
        }

        $sourceTableId = $this->storageApi->createTable($bucketId, $tableId, new CsvFile($filePath));

        $this->storageApi->writeTable($sourceTableId, new CsvFile($filePath));
        $job = $this->storageApi->exportTableAsync(
            $sourceTableId,
            [
                'gzip' => true,
            ]
        );
        $fileInfo = $this->storageApi->getFile(
            $job['file']['id'],
            (new GetFileOptions())->setFederationToken(true)
        );

        if (isset($fileInfo['absPath'])) {
            return [
                'stagingStorage' => self::STORAGE_ABS,
                'manifest' => $this->getAbsManifest($fileInfo),
            ];
        } else {
            return [
                'stagingStorage' => self::STORAGE_S3,
                'manifest' => $this->getS3Manifest($fileInfo),
            ];
        }
    }

    private function getS3Manifest(array $fileInfo): array
    {
        return [
            'isSliced' => $fileInfo['isSliced'],
            'region' => $fileInfo['region'],
            'bucket' => $fileInfo['s3Path']['bucket'],
            'key' => $fileInfo['isSliced']?$fileInfo['s3Path']['key'] . 'manifest':$fileInfo['s3Path']['key'],
            'credentials' => [
                'access_key_id' => $fileInfo['credentials']['AccessKeyId'],
                'secret_access_key' => $fileInfo['credentials']['SecretAccessKey'],
                'session_token' => $fileInfo['credentials']['SessionToken'],
            ],
        ];
    }

    private function getAbsManifest(array $fileInfo): array
    {
        return [
            'is_sliced' => $fileInfo['isSliced'],
            'region' => $fileInfo['region'],
            'container' => $fileInfo['absPath']['container'],
            'name' => $fileInfo['isSliced'] ? $fileInfo['absPath']['name'] . 'manifest' : $fileInfo['absPath']['name'],
            'credentials' => [
                'sas_connection_string' => $fileInfo['absCredentials']['SASConnectionString'],
                'expiration' => $fileInfo['absCredentials']['expiration'],
            ],
        ];
    }
}
