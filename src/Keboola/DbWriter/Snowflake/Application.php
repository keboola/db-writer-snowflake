<?php

namespace Keboola\DbWriter\Snowflake;

use Keboola\Csv\CsvFile;
use \Keboola\DbWriter\Application as BaseApplication;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Writer\Snowflake;

class Application extends BaseApplication
{
    public function runAction()
    {
        $uploaded = [];
        $tables = array_filter($this['parameters']['tables'], function ($table) {
            return ($table['export']);
        });

        /** @var Snowflake $writer */
        $writer = $this['writer'];
        foreach ($tables as $table) {
            if (!$writer->isTableValid($table)) {
                continue;
            }

            $manifest = $this->getManifest($table['tableId']);

            $targetTableName = $table['dbName'];
            if ($table['incremental']) {
                $table['dbName'] = $writer->generateTmpName($table['dbName']);
            }

            $table['items'] = $this->reorderColumns(
                $this->createHeadersCsvFile($manifest['columns']),
                $table['items']
            );

            if (empty($table['items'])) {
                continue;
            }

            try {
                $writer->drop($table['dbName']);
                $writer->create($table);
                $writer->writeFromS3($manifest['s3'], $table);

                if ($table['incremental']) {
                    // create target table if not exists
                    if (!$writer->tableExists($targetTableName)) {
                        $destinationTable = $table;
                        $destinationTable['dbName'] = $targetTableName;
                        $destinationTable['incremental'] = false;
                        $writer->create($destinationTable);
                    }
                    $writer->upsert($table, $targetTableName);
                }
            } catch (Exception $e) {
                throw new UserException($e->getMessage(), 0, $e, ["trace" => $e->getTraceAsString()]);
            } catch (UserException $e) {
                throw $e;
            } catch (\Throwable $e) {
                throw new ApplicationException($e->getMessage(), 2, $e, ["trace" => $e->getTraceAsString()]);
            }

            $uploaded[] = $table['tableId'];
        }

        return "Writer finished successfully";
    }

    private function getManifest($tableId)
    {
        return json_decode(
            (string) file_get_contents($this['parameters']['data_dir'] . "/in/tables/" . $tableId . ".csv.manifest"),
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
