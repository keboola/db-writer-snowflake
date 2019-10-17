<?php

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\DbWriter\Snowflake\Test\S3Loader;
use Keboola\DbWriter\Writer\Snowflake;
use Keboola\StorageApi\Client;
use Symfony\Component\Process\Process;

class FunctionalTest extends BaseTest
{
    private const PROCESS_TIMEOUT_SECONDS = 180;

    protected $dataDir = __DIR__ . '/../data/functional';

    public function setUp(): void
    {
        // cleanup & init
        $this->tmpRunDir = '/tmp/' . uniqid('wr-db-snowflake_');
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

        foreach ($this->config['parameters']['tables'] as $table) {
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
        $this->assertFalse($this->writer->tableExists('simple'));
        $this->assertFalse($this->writer->tableExists('special'));

        foreach ($this->config['parameters']['tables'] as $table) {
            if ($table['dbName'] === 'simple') {
                $this->assertTrue($table['incremental']);
            }

            if ($table['dbName'] === 'special') {
                $this->assertFalse($table['incremental']);
            }
        }

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
            $this->dataDir . '/in/tables/simple_merged.csv',
            $this->createCsvFromTable('simple')->getPathname()
        );

        // full load
        $this->assertTrue($this->writer->tableExists('special'));
        $this->assertFileEquals(
            $this->dataDir . '/in/tables/special.csv',
            $this->createCsvFromTable('special')->getPathname()
        );
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
            unset($config['parameters']['tables']);
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

    public function testForeignKeys(): void
    {
        $this->initConfig(function ($config) {
            $tables = array_map(function ($table) {
                $table['items'] = array_map(function ($item) use ($table) {
                    if ($item['name'] === 'name' && $table['tableId'] === 'simple') {
                        $item['foreignKeyTable'] = 'special';
                        $item['foreignKeyColumn'] = 'col1';
                    }
                    return $item;
                }, $table['items']);
                return $table;
            }, $config['parameters']['tables']);
            $config['parameters']['tables'] = $tables;
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

        $this->writer->checkForeignKey('simple', 'special', 'col1');
    }

    public function testInvalidForeignKeyColumnType(): void
    {
        $this->initConfig(function ($config) {
            $tables = array_map(function ($table) {
                $table['items'] = array_map(function ($item) use ($table) {
                    if ($item['name'] === 'id' && $table['tableId'] === 'simple') {
                        $item['foreignKeyTable'] = 'special';
                        $item['foreignKeyColumn'] = 'col1';
                    }
                    return $item;
                }, $table['items']);
                return $table;
            }, $config['parameters']['tables']);
            $config['parameters']['tables'] = $tables;
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
        $this->assertStringContainsString('Foreign key column \'col1\' in table \'special\' must be the same type as column \'id\' in source table', $process->getOutput());
    }

    public function testInvalidForeignColumn(): void
    {
        $this->initConfig(function ($config) {
            $tables = array_map(function ($table) {
                $table['items'] = array_map(function ($item) use ($table) {
                    if ($item['name'] === 'id' && $table['tableId'] === 'simple') {
                        $item['foreignKeyTable'] = 'special';
                        $item['foreignKeyColumn'] = 'randomcolumn';
                    }
                    return $item;
                }, $table['items']);
                return $table;
            }, $config['parameters']['tables']);
            $config['parameters']['tables'] = $tables;
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
        $this->assertStringContainsString('Column \'randomcolumn\' in table \'special\' not found', $process->getOutput());
    }
}
