<?php

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\DbWriter\Snowflake\Test\S3Loader;
use Keboola\DbWriter\Test\BaseTest;
use Keboola\StorageApi\Client;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class FunctionalTest extends BaseTest
{
    protected $dataDir = ROOT_PATH . 'tests/data/functional';

    protected $tmpRunDir;

    public function setUp(): void
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

        $yaml = new Yaml();

        foreach ($config['parameters']['tables'] as $table) {
            // clean destination DB
            $writer->drop($table['dbName']);

            // upload source files to S3 - mimic functionality of docker-runner
            $srcManifestPath = $this->dataDir . '/in/tables/' . $table['tableId'] . '.csv.manifest';
            $manifestData = $yaml->parse(file_get_contents($srcManifestPath));
            $manifestData['s3'] = $s3Loader->upload($table['tableId']);

            $dstManifestPath = $this->tmpRunDir . '/in/tables/' . $table['tableId'] . '.csv.manifest';
            file_put_contents(
                $dstManifestPath,
                $yaml->dump($manifestData)
            );
        }
    }

    public function testRun(): void
    {
        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . $this->tmpRunDir . ' 2>&1');
        $process->run();

        $this->assertEquals(0, $process->getExitCode(), 'Output: ' . $process->getOutput());
    }

    public function testRunAllIgnored(): void
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

        $yaml = new Yaml();

        foreach ($config['parameters']['tables'] as $table) {
            // upload source files to S3 - mimic functionality of docker-runner
            $srcManifestPath = $this->dataDir . '/in/tables/' . $table['tableId'] . '.csv.manifest';
            $dstManifestPath = $this->tmpRunDir . '/in/tables/' . $table['tableId'] . '.csv.manifest';
            $manifestData = $yaml->parse(file_get_contents($srcManifestPath));
            $manifestData['columns'] = [];

            unlink($dstManifestPath);
            file_put_contents(
                $dstManifestPath,
                $yaml->dump($manifestData)
            );
        }

        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . $this->tmpRunDir);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
    }

    public function testTestConnection(): void
    {
        $this->initConfig(function ($config) {
            $config['action'] = 'testConnection';
            return $config;
        });

        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . $this->tmpRunDir . ' 2>&1');
        $process->run();

        $this->assertEquals(0, $process->getExitCode());

        $data = json_decode($process->getOutput(), true);

        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }

    public function testUserException(): void
    {
        $this->initConfig(function ($config) {
            $config['parameters']['tables'][0]['items'][1]['type'] = 'int';
            return $config;
        });

        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . $this->tmpRunDir . ' 2>&1');
        $process->run();

        $this->assertEquals(1, $process->getExitCode());
    }

    private function initConfig(?callable $callback = null): array
    {
        $yaml = new Yaml();
        $dstConfigPath = $this->tmpRunDir . '/config.yml';
        $config = $yaml->parse(file_get_contents($this->dataDir . '/config.yml'));

        $config['parameters']['writer_class'] = ucfirst(SnowflakeTest::DRIVER);
        $config['parameters']['db']['user'] = $this->getEnv(SnowflakeTest::DRIVER, 'DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv(SnowflakeTest::DRIVER, 'DB_PASSWORD', true);
        $config['parameters']['db']['password'] = $this->getEnv(SnowflakeTest::DRIVER, 'DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv(SnowflakeTest::DRIVER, 'DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv(SnowflakeTest::DRIVER, 'DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv(SnowflakeTest::DRIVER, 'DB_DATABASE');
        $config['parameters']['db']['schema'] = $this->getEnv(SnowflakeTest::DRIVER, 'DB_SCHEMA');
        $config['parameters']['db']['warehouse'] = $this->getEnv(SnowflakeTest::DRIVER, 'DB_WAREHOUSE');


        if ($callback !== null) {
            $config = $callback($config);
        }

        @unlink($dstConfigPath);
        file_put_contents($dstConfigPath, $yaml->dump($config));

        return $config;
    }
}