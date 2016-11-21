<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 27/10/16
 * Time: 17:20
 */

namespace Keboola\DbWriter\Writer\Snowflake\Tests;

use Keboola\DbWriter\Snowflake\Test\S3Loader;
use Keboola\DbWriter\Test\BaseTest;
use Keboola\StorageApi\Client;
use Symfony\Component\Process\Process;
use Symfony\Component\Yaml\Yaml;

class FunctionalTest extends BaseTest
{
    const DRIVER = 'Snowflake';

    protected $dataDir = ROOT_PATH . 'tests/data/functional';

    protected $defaultConfig;

    public function setUp()
    {
        // cleanup & init
        $this->defaultConfig = $this->initConfig();
        $writer = $this->getWriter($this->defaultConfig['parameters']);
        $s3Loader = new S3Loader(
            $this->dataDir,
            new Client([
                'token' => getenv('STORAGE_API_TOKEN')
            ])
        );

        $yaml = new Yaml();
        foreach ($this->defaultConfig['parameters']['tables'] as $table) {
            // clean destination DB
            $writer->drop($table['dbName']);

            // upload source files to S3 - mimic functionality of docker-runner
            $manifestPath = $this->dataDir . '/in/tables/' . $table['tableId'] . '.csv.manifest';
            $manifestData = $yaml->parse(file_get_contents($manifestPath));
            $manifestData['s3'] = $s3Loader->upload($table['tableId']);

            unlink($manifestPath);
            file_put_contents(
                $manifestPath,
                $yaml->dump($manifestData)
            );
        }
    }

    public function testRun()
    {
        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . $this->dataDir . ' 2>&1');
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
    }

    public function testRunEmptyTable()
    {
        $config = $this->initConfig(function () {
            $config = $this->defaultConfig;
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
            $manifestPath = $this->dataDir . '/in/tables/' . $table['tableId'] . '.csv.manifest';
            $manifestData = $yaml->parse(file_get_contents($manifestPath));
            $manifestData['columns'] = [];

            unlink($manifestPath);
            file_put_contents(
                $manifestPath,
                $yaml->dump($manifestData)
            );
        }

        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . $this->dataDir);
        $process->run();

        $this->assertEquals(0, $process->getExitCode());
    }

    public function testTestConnection()
    {
        $this->initConfig(function ($config) {
            $config['action'] = 'testConnection';
            return $config;
        });

        $process = new Process('php ' . ROOT_PATH . 'run.php --data=' . $this->dataDir . ' 2>&1');
        $process->run();

        $this->assertEquals(0, $process->getExitCode());

        $data = json_decode($process->getOutput(), true);

        $this->assertArrayHasKey('status', $data);
        $this->assertEquals('success', $data['status']);
    }

    private function initConfig(callable $callback = null)
    {
        $yaml = new Yaml();
        $configPath = $this->dataDir . '/config.yml';
        $config = $yaml->parse(file_get_contents($configPath));

        $config['parameters']['writer_class'] = self::DRIVER;
        $config['parameters']['db']['user'] = $this->getEnv(self::DRIVER, 'DB_USER', true);
        $config['parameters']['db']['#password'] = $this->getEnv(self::DRIVER, 'DB_PASSWORD', true);
        $config['parameters']['db']['password'] = $this->getEnv(self::DRIVER, 'DB_PASSWORD', true);
        $config['parameters']['db']['host'] = $this->getEnv(self::DRIVER, 'DB_HOST');
        $config['parameters']['db']['port'] = $this->getEnv(self::DRIVER, 'DB_PORT');
        $config['parameters']['db']['database'] = $this->getEnv(self::DRIVER, 'DB_DATABASE');
        $config['parameters']['db']['schema'] = $this->getEnv(self::DRIVER, 'DB_SCHEMA');


        if ($callback !== null) {
            $config = $callback($config);
        }

        @unlink($configPath);
        file_put_contents($configPath, $yaml->dump($config));

        return $config;
    }
}
