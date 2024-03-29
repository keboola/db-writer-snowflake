<?php

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\DbWriter\Snowflake\Test\StagingStorageLoader;
use Keboola\StorageApi\Client;
use Symfony\Component\Process\Process;

class FunctionalRowTest extends BaseTest
{
    private const PROCESS_TIMEOUT_SECONDS = 180;

    public function testRun(): void
    {
        $dataDir = __DIR__ . '/../data/functional-row';
        $this->initDataDir($dataDir);

        $this->assertFalse($this->writer->tableExists('simple'));
        $process = Process::fromShellCommandline(
            'php ' . $this->getEntryPointPathName() . ' --data=' . $this->tmpRunDir . ' 2>&1',
            null,
            null,
            null,
            self::PROCESS_TIMEOUT_SECONDS
        );
        $process->run();

        $this->assertEquals(0, $process->getExitCode(), 'Output: ' . $process->getOutput());
        $this->assertTrue($this->writer->tableExists('simple'));
        $this->assertFileEquals(
            $dataDir . '/in/tables/simple.csv',
            $this->createCsvFromTable('simple')->getPathname()
        );
    }

    public function testNumericDefaultValue(): void
    {
        $dataDir = __DIR__ . '/../data/numeric-default-value';
        $this->initDataDir($dataDir);

        $this->assertFalse($this->writer->tableExists('numeric'));
        $process = Process::fromShellCommandline(
            'php ' . $this->getEntryPointPathName() . ' --data=' . $this->tmpRunDir . ' 2>&1',
            null,
            null,
            null,
            self::PROCESS_TIMEOUT_SECONDS
        );
        $process->run();

        $this->assertEquals(0, $process->getExitCode(), 'Output: ' . $process->getOutput());
        $this->assertTrue($this->writer->tableExists('numeric'));
        $this->assertFileEquals(
            __DIR__ . '/../data/numeric.expected.csv',
            $this->createCsvFromTable('numeric')->getPathname()
        );
    }

    public function testTestConnection(): void
    {
        $dataDir = __DIR__ . '/../data/functional-row';
        $this->initDataDir($dataDir);

        $this->initConfig(function ($config) {
            $config['action'] = 'testConnection';
            $config['parameters'] = array_filter($config['parameters'], function ($key) {
                if (in_array($key, ['data_dir'. 'writer_class', 'db'])) {
                    return true;
                }
                return false;
            }, ARRAY_FILTER_USE_KEY);
            return $config;
        });

        $process = Process::fromShellCommandline(
            'php ' . $this->getEntryPointPathName() . ' --data=' . $this->tmpRunDir . ' 2>&1',
            null,
            null,
            null,
            self::PROCESS_TIMEOUT_SECONDS
        );
        $process->run();

        $this->assertEquals(0, $process->getExitCode(), $process->getOutput() . $process->getErrorOutput());

        $data = json_decode($process->getOutput(), true);

        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }

    public function testInvalidWarehouse(): void
    {
        $dataDir = __DIR__ . '/../data/functional-row';
        $this->initDataDir($dataDir);

        $this->initConfig(function ($config) {
            $config['parameters']['db']['warehouse'] = uniqid();
            return $config;
        });

        $process = Process::fromShellCommandline(
            'php ' . $this->getEntryPointPathName() . ' --data=' . $this->tmpRunDir . ' 2>&1',
            null,
            null,
            null,
            self::PROCESS_TIMEOUT_SECONDS
        );
        $process->run();

        $this->assertEquals(1, $process->getExitCode());
        $this->assertStringContainsString('Invalid warehouse', $process->getOutput());
    }

    public function testInvalidSchema(): void
    {
        $dataDir = __DIR__ . '/../data/functional-row';
        $this->initDataDir($dataDir);

        $this->initConfig(function ($config) {
            $config['parameters']['db']['schema'] = uniqid();
            return $config;
        });

        $process = Process::fromShellCommandline(
            'php ' . $this->getEntryPointPathName() . ' --data=' . $this->tmpRunDir . ' 2>&1',
            null,
            null,
            null,
            self::PROCESS_TIMEOUT_SECONDS
        );
        $process->run();

        $this->assertEquals(1, $process->getExitCode());
        $this->assertStringContainsString('Invalid schema', $process->getOutput());
    }

    protected function initDataDir(string $dataDir): void
    {
        // cleanup & init
        $this->dataDir = $dataDir;
        $this->tmpRunDir = '/tmp/' . uniqid('wr-db-snowflake_row_');
        mkdir($this->tmpRunDir . '/in/tables/', 0777, true);
        $this->config = $this->initConfig();

        $this->writer = $this->getSnowflakeWriter($this->config['parameters']);
        $stagingStorageLoader = new StagingStorageLoader(
            $dataDir,
            new Client([
                'url' => getenv('KBC_URL'),
                'token' => getenv('STORAGE_API_TOKEN'),
            ])
        );

        // clean destination DB
        $this->writer->drop($this->config['parameters']['dbName']);

        // upload source files to storage (S3/ABS) - mimic functionality of docker-runner
        $srcManifestPath = $dataDir . '/in/tables/' . $this->config['parameters']['tableId'] . '.csv.manifest';
        $manifestData = json_decode((string) file_get_contents($srcManifestPath), true);
        $uploadFileInfo = $stagingStorageLoader->upload($this->config['parameters']['tableId']);
        $manifestData[$uploadFileInfo['stagingStorage']] = $uploadFileInfo['manifest'];

        $dstManifestPath = $this->tmpRunDir . '/in/tables/' . $this->config['parameters']['tableId'] . '.csv.manifest';
        file_put_contents(
            $dstManifestPath,
            json_encode($manifestData)
        );
    }
}
