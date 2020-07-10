<?php

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Test\BaseTest as CommonBaseTest;
use Keboola\DbWriter\Writer\Snowflake;

abstract class BaseTest extends CommonBaseTest
{
    /** @var Snowflake */
    protected $writer;

    /** @var array */
    protected $config;

    /** @var string */
    protected $tmpRunDir;

    protected function getConfig(?string $dataDir = null): array
    {
        $config = parent::getConfig($dataDir);
        $config['parameters']['writer_class'] = Snowflake::WRITER;
        $config['parameters']['db']['schema'] = $this->getEnv('DB_SCHEMA');
        $config['parameters']['db']['warehouse'] = $this->getEnv('DB_WAREHOUSE');
        $config['parameters']['db']['password'] = $config['parameters']['db']['#password'];

        return $config;
    }

    protected function initConfig(?callable $callback = null): array
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

    protected function getEntryPointPathName(): string
    {
        return __DIR__ . '/../../run.php';
    }

    protected function createCsvFromTable(string $table): CsvFile
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
