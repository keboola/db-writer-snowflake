<?php

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Snowflake\Test\S3Loader;
use Keboola\DbWriter\Writer\Snowflake;
use Keboola\StorageApi\Client;
use Symfony\Component\Process\Process;

class FunctionalRowTest extends BaseTest
{
    private const PROCESS_TIMEOUT_SECONDS = 180;

    protected $dataDir = __DIR__ . '/../data/functional-row';

    /** @var Snowflake */
    private $writer;

    private $config;

    protected $tmpRunDir;

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

    private function createCsvFromTable(string $table)
    {
        $csv = new CsvFile(tempnam('/tmp', 'db-wr-test-tmp'));
        $res = $this->writer->getSnowflakeConnection()->fetchAll(sprintf(
            'SELECT * FROM "%s" ORDER BY 1 ASC',
            $table
        ));

        $i = 0;
        foreach ($res as $row) {
            if ($i === 0) {
                $csv->writeRow(array_keys($row));
            }

            $csv->writeRow($row);
            $i++;
        }

        return $csv;
    }
}
