<?php

namespace Keboola\DbWriter\Snowflake;

use Keboola\Csv\CsvFile;
use \Keboola\DbWriter\Application as BaseApplication;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Snowflake\Configuration\ActionConfigRowDefinition;
use Keboola\DbWriter\Snowflake\Configuration\ConfigDefinition;
use Keboola\DbWriter\Snowflake\Configuration\ConfigRowDefinition;
use Keboola\DbWriter\Writer\Snowflake;

class Application extends BaseApplication
{
    public function __construct(array $config, Logger $logger)
    {
        $action = !is_null($config['action']) ?: 'run';
        if (isset($config['parameters']['tables'])) {
            $configDefinition = new ConfigDefinition();
        } else {
            if ($action === 'run') {
                $configDefinition = new ConfigRowDefinition();
            } else {
                $configDefinition = new ActionConfigRowDefinition();
            }
        }

        parent::__construct($config, $logger, $configDefinition);
    }

    public function runAction(): string
    {
        if (isset($this['parameters']['tables'])) {
            $tables = array_filter((array) $this['parameters']['tables'], function ($table) {
                return ($table['export']);
            });
            foreach ($tables as $key => $tableConfig) {
                $tables[$key] = $this->processRunAction($tableConfig);
            }
            foreach ($tables as $table) {
                /** @var Snowflake $writer */
                $writer = $this['writer'];
                $writer->createForeignKeys($table);
            }
        } elseif (!isset($this['parameters']['export']) || $this['parameters']['export']) {
            $this->processRunAction($this['parameters']);
        }
        return 'Writer finished successfully';
    }

    private function processRunAction(array $tableConfig): array
    {
        $manifest = $this->getManifest($tableConfig['tableId']);

        $tableConfig['items'] = $this->reorderColumns(
            $this->createHeadersCsvFile($manifest['columns']),
            $tableConfig['items']
        );

        if (empty($tableConfig['items'])) {
            return $tableConfig;
        }

        try {
            if (isset($tableConfig['incremental']) && $tableConfig['incremental']) {
                $this->writeIncrementalFromS3($manifest['s3'], $tableConfig);
            } else {
                $this->writeFullFromS3($manifest['s3'], $tableConfig);
            }
        } catch (Exception $e) {
            $this['logger']->error($e->getMessage());
            throw new UserException($e->getMessage(), 0, $e);
        } catch (UserException $e) {
            $this['logger']->error($e->getMessage());
            throw $e;
        } catch (\Throwable $e) {
            throw new ApplicationException($e->getMessage(), 2, $e);
        }

        return $tableConfig;
    }

    public function writeFull(CsvFile $csv, array $tableConfig): void
    {
        throw new ApplicationException('Method not implemented');
    }

    public function writeIncremental(CsvFile $csv, array $tableConfig): void
    {
        throw new ApplicationException('Method not implemented');
    }

    public function writeIncrementalFromS3(array $s3info, array $tableConfig): void
    {
        /** @var Snowflake $writer */
        $writer = $this['writer'];

        // write to staging table
        $stageTable = $tableConfig;
        $stageTable['dbName'] = $writer->generateTmpName($tableConfig['dbName']);

        $writer->drop($stageTable['dbName']);
        $writer->createStaging($stageTable);
        $writer->writeFromS3($s3info, $stageTable);

        // create destination table if not exists
        $dstTableExists = $writer->tableExists($tableConfig['dbName']);
        if (!$dstTableExists) {
            $writer->create($tableConfig);
        }
        $writer->validateTable($tableConfig);

        // upsert from staging to destination table
        $writer->upsert($stageTable, $tableConfig['dbName']);
    }

    public function writeFullFromS3(array $s3info, array $tableConfig): void
    {
        /** @var Snowflake $writer */
        $writer = $this['writer'];

        $stagingTableName = uniqid('staging');
        $stagingTableConfig = array_merge($tableConfig, [
            'dbName' => $stagingTableName,
        ]);
        $writer->create($stagingTableConfig);
        try {
            // create dummy table for first load which will be replaced by tables swap
            $writer->createIfNotExists($tableConfig);
            $writer->writeFromS3($s3info, $stagingTableConfig);
            $writer->swapTables($tableConfig['dbName'], $stagingTableName);
        } finally {
            $writer->drop($stagingTableName);
        }
    }

    private function getManifest(string $tableId): array
    {
        return json_decode(
            (string) file_get_contents($this['parameters']['data_dir'] . '/in/tables/' . $tableId . '.csv.manifest'),
            true
        );
    }

    private function createHeadersCsvFile(array $columns): CsvFile
    {
        $fileName = sys_get_temp_dir() . DIRECTORY_SEPARATOR . uniqid('csv_headers_');

        $csv = (new CsvFile($fileName))->writeRow($columns);
        unset($csv);

        return new CsvFile($fileName);
    }
}
