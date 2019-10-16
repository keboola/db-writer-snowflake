<?php

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\DbWriter\Snowflake\Test\S3Loader;
use Keboola\DbWriter\Writer\Snowflake;
use Keboola\StorageApi\Client;
use Symfony\Component\Process\Process;

class FunctionalRowTest extends BaseTest
{
    private const PROCESS_TIMEOUT_SECONDS = 180;

    protected $dataDir = __DIR__ . '/../data/functional-row';

    public function setUp(): void
    {
        // cleanup & init
        $this->tmpRunDir = '/tmp/' . uniqid('wr-db-snowflake_row_');
        mkdir($this->tmpRunDir . '/in/tables/', 0777, true);
        $this->config = $this->initConfig();

        $writer = $this->getWriter($this->config['parameters']);
        if ($writer instanceof Snowflake) {
            $this->writer = $writer;
        } else {
            $this->fail('Writer factory must init Snowflake Writer');
        }

        $writer = $this->getWriter($this->config['parameters']);
        $s3Loader = new S3Loader(
            $this->dataDir,
            new Client([
                'token' => getenv('STORAGE_API_TOKEN'),
            ])
        );

        // clean destination DB
        $writer->drop($this->config['parameters']['dbName']);

        // upload source files to S3 - mimic functionality of docker-runner
        $srcManifestPath = $this->dataDir . '/in/tables/' . $this->config['parameters']['tableId'] . '.csv.manifest';
        $manifestData = json_decode((string) file_get_contents($srcManifestPath), true);
        $manifestData['s3'] = $s3Loader->upload($this->config['parameters']['tableId']);

        $dstManifestPath = $this->tmpRunDir . '/in/tables/' . $this->config['parameters']['tableId'] . '.csv.manifest';
        file_put_contents(
            $dstManifestPath,
            json_encode($manifestData)
        );
    }

    public function testRun()
    {
        $this->assertFalse($this->writer->tableExists('simple'));
        $process = new Process(
            'php ' . $this->getEntryPointPathName() . ' --data=' . $this->tmpRunDir . ' 2>&1',
            null,
            null,
            null,
            self::PROCESS_TIMEOUT_SECONDS
        );
        $process->run();

        $this->assertEquals(0, $process->getExitCode(), 'Output: ' . $process->getOutput());

        // incremental load
        $this->assertTrue($this->writer->tableExists('simple'));
        $this->assertFileEquals(
            $this->dataDir . '/in/tables/simple.csv',
            $this->createCsvFromTable('simple')->getPathname()
        );
    }

    public function testTestConnection()
    {
        $this->initConfig(function ($config) {
            $config['action'] = 'testConnection';
            return $config;
        });

        $process = new Process(
            'php ' . $this->getEntryPointPathName() . ' --data=' . $this->tmpRunDir . ' 2>&1',
            null,
            null,
            null,
            self::PROCESS_TIMEOUT_SECONDS
        );
        $process->run();

        $this->assertEquals(0, $process->getExitCode());

        $data = json_decode($process->getOutput(), true);

        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }

    public function testInvalidWarehouse(): void
    {
        $this->initConfig(function ($config) {
            $config['action'] = 'testConnection';
            $config['parameters']['db']['warehouse'] = uniqid();
            return $config;
        });

        $process = new Process(
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
        $this->initConfig(function ($config) {
            $config['action'] = 'testConnection';
            $config['parameters']['db']['schema'] = uniqid();
            return $config;
        });

        $process = new Process(
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
}
