<?php

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\DbWriter\Snowflake\Test\S3Loader;
use Keboola\StorageApi\Client;
use Symfony\Component\Process\Process;

class FunctionalTest extends BaseTest
{
    private const PROCESS_TIMEOUT_SECONDS = 180;

    protected $dataDir = __DIR__ . '/../data/functional';

    protected $tmpRunDir;

    public function setUp()
    {
        // cleanup & init
        $this->tmpRunDir = '/tmp/' . uniqid('wr-db-snowflake_');
        mkdir($this->tmpRunDir . '/in/tables/', 0777, true);
        $config = $this->initConfig();

        $writer = $this->getWriter($config['parameters']);
        $s3Loader = new S3Loader(
            $this->dataDir,
            new Client([
                'token' => getenv('STORAGE_API_TOKEN'),
            ])
        );

        foreach ($config['parameters']['tables'] as $table) {
            // clean destination DB
            $writer->drop($table['dbName']);

            // upload source files to S3 - mimic functionality of docker-runner
            $srcManifestPath = $this->dataDir . '/in/tables/' . $table['tableId'] . '.csv.manifest';
            $manifestData = json_decode((string) file_get_contents($srcManifestPath), true);
            $manifestData['s3'] = $s3Loader->upload($table['tableId']);

            $dstManifestPath = $this->tmpRunDir . '/in/tables/' . $table['tableId'] . '.csv.manifest';
            file_put_contents(
                $dstManifestPath,
                json_encode($manifestData)
            );
        }
    }

    public function testRun()
    {
        $process = new Process(
            'php ' . $this->getEntryPointPathName() . ' --data=' . $this->tmpRunDir . ' 2>&1',
            null,
            null,
            null,
            self::PROCESS_TIMEOUT_SECONDS
        );
        $process->run();

        $this->assertEquals(0, $process->getExitCode(), 'Output: ' . $process->getOutput());
    }

    public function testRunAllIgnored()
    {
        $config = $this->initConfig(function ($config) {
            $tables = array_map(function ($table) {
                $table['items'] = array_map(function ($item) {
                    $item['type'] = 'IGNORE';
                    return $item;
                }, $table['items']);
                return $table;
            }, $config['parameters']['tables']);
            $config['parameters']['tables'] = $tables;

            return $config;
        });

        foreach ($config['parameters']['tables'] as $table) {
            // upload source files to S3 - mimic functionality of docker-runner
            $srcManifestPath = $this->dataDir . '/in/tables/' . $table['tableId'] . '.csv.manifest';
            $dstManifestPath = $this->tmpRunDir . '/in/tables/' . $table['tableId'] . '.csv.manifest';
            $manifestData = json_decode((string) file_get_contents($srcManifestPath), true);
            $manifestData['columns'] = [];

            unlink($dstManifestPath);
            file_put_contents(
                $dstManifestPath,
                json_encode($manifestData)
            );
        }

        $process = new Process(
            'php ' . $this->getEntryPointPathName() . ' --data=' . $this->tmpRunDir,
            null,
            null,
            null,
            self::PROCESS_TIMEOUT_SECONDS
        );
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
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

    public function testUserException()
    {
        $this->initConfig(function ($config) {
            $config['parameters']['tables'][0]['items'][1]['type'] = 'int';
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
        $this->assertContains('Invalid warehouse', $process->getOutput());
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
        $this->assertContains('Invalid schema', $process->getOutput());
    }

    private function initConfig(?callable $callback = null)
    {
        $dstConfigPath = $this->tmpRunDir . '/config.json';

        $config = $this->getConfig($this->dataDir);
        if ($callback !== null) {
            $config = $callback($config);
        }

        @unlink($dstConfigPath);
        file_put_contents($dstConfigPath, json_encode($config));

        return $config;
    }

    private function getEntryPointPathName(): string
    {
        return __DIR__ . '/../../run.php';
    }
}
