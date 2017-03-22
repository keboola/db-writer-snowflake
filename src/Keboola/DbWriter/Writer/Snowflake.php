<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 12/02/16
 * Time: 16:38
 */

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Snowflake\Connection;
use Keboola\DbWriter\Writer;
use Keboola\DbWriter\WriterInterface;

class Snowflake extends Writer implements WriterInterface
{
    const STATEMENT_TIMEOUT_IN_SECONDS = 900;
    const STAGE_NAME = 'db-writer';

    private static $allowedTypes = [
        'number',
        'decimal', 'numeric',
        'int', 'integer', 'bigint', 'smallint', 'tinyint', 'byteint',
        'float', 'float4', 'float8',
        'double', 'double precision', 'real',
        'boolean',
        'char', 'character', 'varchar', 'string', 'text', 'binary',
        'date', 'time', 'timestamp', 'timestamp_ltz', 'timestamp_ntz', 'timestamp_tz'
    ];

    private static $typesWithSize = [
        'number', 'decimal', 'numeric',
        'char', 'character', 'varchar', 'string', 'text', 'binary'
    ];

    /** @var Connection */
    protected $db;

    private $dbParams;

    /** @var Logger */
    protected $logger;

    public function __construct($dbParams, Logger $logger)
    {
        parent::__construct($dbParams, $logger);
        $this->dbParams = $dbParams;
        $this->logger = $logger;
    }

    public function createConnection($dbParams)
    {
        $connection = new Connection($dbParams);
        $connection->query(sprintf("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = %d", self::STATEMENT_TIMEOUT_IN_SECONDS));
        return $connection;
    }

    public function writeFromS3($s3info, array $table)
    {
        $this->execQuery($this->generateCreateStageCommand($s3info));
        $this->execQuery($this->generateCopyCommand($table['dbName'], $s3info, $table['items']));
    }

    private function generateCreateStageCommand($s3info)
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', $this->quote(','));
        $csvOptions[] = sprintf("FIELD_OPTIONALLY_ENCLOSED_BY = %s", $this->quote('"'));
        $csvOptions[] = sprintf("ESCAPE_UNENCLOSED_FIELD = %s", $this->quote('\\'));

        if (!$s3info['isSliced']) {
            $csvOptions[] = "SKIP_HEADER = 1";
        }

        return sprintf(
            "CREATE OR REPLACE STAGE %s
             FILE_FORMAT = (TYPE=CSV %s)
             URL = 's3://%s'
             CREDENTIALS = (AWS_KEY_ID = %s AWS_SECRET_KEY = %s  AWS_TOKEN = %s)
            ",
            $this->quoteIdentifier(self::STAGE_NAME),
            implode(' ', $csvOptions),
            $s3info['bucket'],
            $this->quote($s3info['credentials']['access_key_id']),
            $this->quote($s3info['credentials']['secret_access_key']),
            $this->quote($s3info['credentials']['session_token'])
        );
    }

    private function generateCopyCommand($tableName, $s3info, $columns)
    {
        $columnNames = array_map(function ($column) {
            return $this->quoteIdentifier($column['dbName']);
        }, $columns);

        $transformationColumns = array_map(
            function ($column, $index) {
                if (!empty($column['nullable'])) {
                    return sprintf("IFF(t.$%d = '', null, t.$%d)", $index + 1, $index + 1);
                }
                return sprintf('t.$%d', $index + 1);
            },
            $columns,
            array_keys($columns)
        );

        $path = $s3info['key'];
        $pattern = '';
        if ($s3info['isSliced']) {
            // key ends with manifest
            if (strrpos($s3info['key'], 'manifest') === strlen($s3info['key']) - strlen('manifest')) {
                $path = substr($s3info['key'], 0, strlen($s3info['key']) - strlen('manifest'));
                $pattern = 'PATTERN="^.*(?<!manifest)$"';
            }
        }

        return sprintf(
            "COPY INTO %s(%s) 
            FROM (SELECT %s FROM %s t)
            %s",
            $this->nameWithSchemaEscaped($tableName),
            implode(', ', $columnNames),
            implode(', ', $transformationColumns),
            $this->quote('@' . $this->quoteIdentifier(self::STAGE_NAME) . "/" . $path),
            $pattern
        );
    }


    protected function nameWithSchemaEscaped($tableName, $schemaName = null)
    {
        if ($schemaName === null) {
            $schemaName = $this->dbParams['schema'];
        }
        return sprintf(
            '%s.%s',
            $this->quoteIdentifier($schemaName),
            $this->quoteIdentifier($tableName)
        );
    }

    private function quote($value)
    {
        return "'" . addslashes($value) . "'";
    }

    private function quoteIdentifier($value)
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }

    public function isTableValid(array $table, $ignoreExport = false)
    {
        // TODO: Implement isTableValid() method.

        return true;
    }

    public function drop($tableName)
    {
        $this->execQuery(sprintf("DROP TABLE IF EXISTS %s;", $this->escape($tableName)));
    }

    public function create(array $table)
    {
        $sql = sprintf(
            "CREATE %s TABLE %s (",
            $table['incremental']?'TEMPORARY':'',
            $this->escape($table['dbName'])
        );

        $columns = array_filter($table['items'], function ($item) {
            return (strtolower($item['type']) !== 'ignore');
        });
        foreach ($columns as $col) {
            $type = strtoupper($col['type']);
            if (!empty($col['size']) && in_array(strtolower($col['type']), self::$typesWithSize)) {
                $type .= sprintf("(%s)", $col['size']);
            }
            $null = $col['nullable'] ? 'NULL' : 'NOT NULL';
            $default = empty($col['default']) ? '' : "DEFAULT '{$col['default']}'";
            if ($type == 'TEXT') {
                $default = '';
            }
            $sql .= sprintf(
                "%s %s %s %s,",
                $this->escape($col['dbName']),
                $type,
                $null,
                $default
            );
        }
        $sql = substr($sql, 0, -1);
        $sql .= ");";

        $this->execQuery($sql);
    }

    public function upsert(array $table, $targetTable)
    {
        $sourceTable = $this->nameWithSchemaEscaped($table['dbName']);
        $targetTable = $this->nameWithSchemaEscaped($targetTable);

        $columns = array_map(
            function ($item) {
                return $this->quoteIdentifier($item['dbName']);
            },
            array_filter($table['items'], function ($item) {
                return strtolower($item['type']) != 'ignore';
            })
        );

        if (!empty($table['primaryKey'])) {
            // update data
            $joinClauseArr = [];
            foreach ($table['primaryKey'] as $index => $value) {
                $joinClauseArr[] = sprintf(
                    '%s.%s=%s.%s',
                    $targetTable,
                    $this->quoteIdentifier($value),
                    $sourceTable,
                    $this->quoteIdentifier($value)
                );
            }
            $joinClause = implode(' AND ', $joinClauseArr);

            $valuesClauseArr = [];
            foreach ($columns as $index => $column) {
                $valuesClauseArr[] = sprintf(
                    '%s=%s.%s',
                    $column,
                    $sourceTable,
                    $column
                );
            }
            $valuesClause = implode(',', $valuesClauseArr);

            $this->execQuery(sprintf(
                "UPDATE %s SET %s FROM %s WHERE %s",
                $targetTable,
                $valuesClause,
                $sourceTable,
                $joinClause
            ));

            // delete updated from temp table
            $this->execQuery(sprintf(
                "DELETE FROM %s USING %s WHERE %s",
                $sourceTable,
                $targetTable,
                $joinClause
            ));
        }

        // insert new data
        $columnsClause = implode(',', $columns);
        $query = "INSERT INTO {$targetTable} ({$columnsClause}) SELECT * FROM {$sourceTable}";
        $this->execQuery($query);

        // drop temp table
        $this->drop($table['dbName']);
    }

    public static function getAllowedTypes()
    {
        return self::$allowedTypes;
    }

    public function tableExists($tableName)
    {
        $res = $this->db->fetchAll(sprintf(
            "SELECT *
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_name = '%s'",
            $tableName
        ));

        return !empty($res);
    }

    private function execQuery($query)
    {
        $this->logger->info(sprintf("Executing query '%s'", $this->hideCredentialsInQuery($query)));
        try {
            $this->db->query($query);
        } catch (\Exception $e) {
            throw new UserException("Query execution error: " . $e->getMessage(), 0, $e);
        }
    }

    public function showTables($dbName)
    {
        throw new ApplicationException("Method not implemented");
    }

    public function getTableInfo($tableName)
    {
        throw new ApplicationException("Method not implemented");
    }

    public function write(CsvFile $csv, array $table)
    {
        throw new ApplicationException("Method not implemented");
    }

    private function escape($str)
    {
        return '"' . $str . '"';
    }

    private function getUserDefaultWarehouse()
    {
        $sql = sprintf(
            "DESC USER %s;",
            $this->db->quoteIdentifier($this->dbParams['user'])
        );

        $config = $this->db->fetchAll($sql);

        foreach ($config as $item) {
            if ($item['property'] === 'DEFAULT_WAREHOUSE') {
                return $item['value'] === 'null' ? null : $item['value'];
            }
        }

        return null;
    }

    public function testConnection()
    {
        $this->execQuery('SELECT current_date;');

        $envWarehouse = !empty($this->dbParams['warehouse']) ? $this->dbParams['warehouse'] : null;
        $defaultWarehouse = $this->getUserDefaultWarehouse();
        if (!$defaultWarehouse && !$envWarehouse) {
            throw new UserException('Specify "warehouse" parameter');
        }

        $warehouse = $defaultWarehouse;
        if ($envWarehouse) {
            $warehouse = $envWarehouse;
        }

        try {
            $this->db->query(sprintf(
                'USE WAREHOUSE %s;',
                $this->db->quoteIdentifier($warehouse)
            ));
        } catch (\Exception $e) {
            if (preg_match('/Object does not exist/ui', $e->getMessage())) {
                throw new UserException(sprintf('Invalid warehouse "%s" specified', $warehouse));
            } else {
                throw $e;
            }
        }

    }

    public function generateTmpName($tableName)
    {
        return '__temp_' . str_replace('.', '_', uniqid('wr_db_', true));
    }

    private function hideCredentialsInQuery($query)
    {
        return preg_replace("/(AWS_[A-Z_]*\\s=\\s.)[0-9A-Za-z\\/\\+=]*./", '${1}...\'', $query);
    }
}
