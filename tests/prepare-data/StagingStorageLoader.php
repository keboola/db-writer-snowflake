<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake\PrepareTestsData;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\GetFileOptions;
use PHPUnit\Framework\Assert;

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
        return sprintf($this->dataDir . '/%s.csv', $tableId);
    }

    public function upload(string $table): array
    {
        $filePath = $this->getInputCsv($table);
        $bucketId = 'in.c-test-wr-db-snowflake';
        $tableId = $bucketId . '.' . $table;

        if (!$this->storageApi->bucketExists($bucketId)) {
            $this->storageApi->createBucket('test-wr-db-snowflake', Client::STAGE_IN, '', 'snowflake');
        }
        if ($this->storageApi->tableExists($tableId)) {
            $this->storageApi->dropTable($tableId);
        }

        $sourceTableId = $this->storageApi->createTableAsync($bucketId, $table, new CsvFile($filePath));

        $job = $this->storageApi->exportTableAsync(
            $sourceTableId,
            [
                'gzip' => true,
            ],
        );
        $fileInfo = $this->storageApi->getFile(
            $job['file']['id'],
            (new GetFileOptions())->setFederationToken(true),
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
        // File is always exported to stage storage as sliced
        Assert::assertTrue($fileInfo['isSliced']);

        return [
            'isSliced' => $fileInfo['isSliced'],
            'region' => $fileInfo['region'],
            'bucket' => $fileInfo['s3Path']['bucket'],
            'key' => $fileInfo['s3Path']['key'] . 'manifest',
            'credentials' => [
                'access_key_id' => $fileInfo['credentials']['AccessKeyId'],
                'secret_access_key' => $fileInfo['credentials']['SecretAccessKey'],
                'session_token' => $fileInfo['credentials']['SessionToken'],
            ],
        ];
    }

    private function getAbsManifest(array $fileInfo): array
    {
        // File is always exported to stage storage as sliced
        Assert::assertTrue($fileInfo['isSliced']);

        return [
            'is_sliced' => $fileInfo['isSliced'],
            'region' => $fileInfo['region'],
            'container' => $fileInfo['absPath']['container'],
            'name' => $fileInfo['absPath']['name'] . 'manifest',
            'credentials' => [
                'sas_connection_string' => $fileInfo['absCredentials']['SASConnectionString'],
                'expiration' => $fileInfo['absCredentials']['expiration'],
            ],
        ];
    }
}
