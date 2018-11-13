<?php

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Snowflake\Connection;
use Keboola\DbWriter\Writer;
use Keboola\DbWriter\WriterInterface;
use Keboola\DbWriter\Snowflake\DataType;

class Snowflake extends Writer implements WriterInterface
{
    public const WRITER = 'Snowflake';
    private const STATEMENT_TIMEOUT_IN_SECONDS = 3600;
    public const STAGE_NAME = 'db-writer';

    private static $allowedTypes = [
        'number',
        'decimal', 'numeric',
        'int', 'integer', 'bigint', 'smallint', 'tinyint', 'byteint',
        'float', 'float4', 'float8',
        'double', 'double precision', 'real',
        'boolean',
        'char', 'character', 'varchar', 'string', 'text', 'binary',
        'date', 'time', 'timestamp', 'timestamp_ltz', 'timestamp_ntz', 'timestamp_tz',
    ];

    private static $typesWithSize = [
        'number', 'decimal', 'numeric',
        'char', 'character', 'varchar', 'string', 'text', 'binary',
    ];

    /** @var Connection */
    protected $db;

    /** @var Logger */
    protected $logger;

    public function __construct($dbParams, Logger $logger)
    {
        $this->logger = $logger;
        $this->dbParams = $dbParams;

        try {
            $this->db = $this->createSnowflakeConnection($this->dbParams);
        } catch (\Throwable $e) {
            if (strstr(strtolower($e->getMessage()), 'could not find driver')) {
                throw new ApplicationException("Missing driver: " . $e->getMessage());
            }
            throw new UserException("Error connecting to DB: " . $e->getMessage(), 0, $e);
        }

        $this->validateAndSetWarehouse();
        $this->validateAndSetSchema();
    }

    public function createConnection(array $dbParams): \PDO
    {
        throw new ApplicationException("Method not implemented");
    }

    public function createSnowflakeConnection($dbParams): Connection
    {
        $connection = new Connection($dbParams);
        $connection->query(sprintf("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = %d", self::STATEMENT_TIMEOUT_IN_SECONDS));
        return $connection;
    }

    public function writeFromS3($s3info, array $table)
    {
        $this->execQuery($this->generateDropStageCommand()); // remove old db wr stage

        $stageName = $this->generateStageName((string) getenv('KBC_RUNID'));

        $this->execQuery($this->generateCreateStageCommand($stageName, $s3info));

        try {
            $this->execQuery($this->generateCopyCommand($table['dbName'], $stageName, $s3info, $table['items']));
        } catch (UserException $e) {
            $this->execQuery($this->generateDropStageCommand($stageName));
            throw $e;
        }

        $this->execQuery($this->generateDropStageCommand($stageName));
    }

    private function generateDropStageCommand($stage = self::STAGE_NAME)
    {
        return sprintf(
            "DROP STAGE IF EXISTS %s",
            $this->db->quoteIdentifier($stage)
        );
    }

    private function generateCreateStageCommand($stageName, $s3info)
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', $this->db->quote(','));
        $csvOptions[] = sprintf("FIELD_OPTIONALLY_ENCLOSED_BY = %s", $this->db->quote('"'));
        $csvOptions[] = sprintf("ESCAPE_UNENCLOSED_FIELD = %s", $this->db->quote('\\'));

        if (!$s3info['isSliced']) {
            $csvOptions[] = "SKIP_HEADER = 1";
        }

        return sprintf(
            "CREATE OR REPLACE STAGE %s
             FILE_FORMAT = (TYPE=CSV %s)
             URL = 's3://%s'
             CREDENTIALS = (AWS_KEY_ID = %s AWS_SECRET_KEY = %s  AWS_TOKEN = %s)
            ",
            $this->db->quoteIdentifier($stageName),
            implode(' ', $csvOptions),
            $s3info['bucket'],
            $this->db->quote($s3info['credentials']['access_key_id']),
            $this->db->quote($s3info['credentials']['secret_access_key']),
            $this->db->quote($s3info['credentials']['session_token'])
        );
    }

    private function generateCopyCommand($tableName, $stageName, $s3info, $columns)
    {
        $columnNames = array_map(function ($column) {
            return $this->db->quoteIdentifier($column['dbName']);
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
            $this->db->quote('@' . $this->db->quoteIdentifier($stageName) . "/" . $path),
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
            $this->db->quoteIdentifier($schemaName),
            $this->db->quoteIdentifier($tableName)
        );
    }

    public function drop(string $tableName): void
    {
        $this->execQuery(sprintf("DROP TABLE IF EXISTS %s;", $this->db->quoteIdentifier($tableName)));
    }

    public function truncate(string $tableName): void
    {
        $this->execQuery(sprintf("TRUNCATE TABLE %s;", $this->db->quoteIdentifier($tableName)));
    }

    public function create(array $table): void
    {
        $sqlDefinitions = [$this->getColumnsSqlDefinition($table)];
        if (!empty($table['primaryKey'])) {
            $sqlDefinitions [] = $this->getPrimaryKeySqlDefinition($table['primaryKey']);
        }

        $this->execQuery(sprintf(
            "CREATE TABLE %s (%s);",
            $this->db->quoteIdentifier($table['dbName']),
            implode(', ', $sqlDefinitions)
        ));
    }

    public function createStaging(array $table): void
    {
        $sqlDefinitions = [$this->getColumnsSqlDefinition($table)];
        if (!empty($table['primaryKey'])) {
            $sqlDefinitions [] = $this->getPrimaryKeySqlDefinition($table['primaryKey']);
        }

        $this->execQuery(sprintf(
            "CREATE TEMPORARY TABLE %s (%s);",
            $this->db->quoteIdentifier($table['dbName']),
            implode(', ', $sqlDefinitions)
        ));
    }

    public function upsert(array $table, string $targetTable): void
    {
        if (!empty($table['primaryKey'])) {
            $this->addPrimaryKeyIfMissing($table['primaryKey'], $targetTable);

            // check primary keys
            $this->checkPrimaryKey($table['primaryKey'], $targetTable);
        }

        $sourceTable = $this->nameWithSchemaEscaped($table['dbName']);
        $targetTable = $this->nameWithSchemaEscaped($targetTable);

        $columns = array_map(
            function ($item) {
                return $this->db->quoteIdentifier($item['dbName']);
            },
            array_filter($table['items'], function ($item) {
                return strtolower($item['type']) !== 'ignore';
            })
        );

        if (!empty($table['primaryKey'])) {
            // update data
            $joinClauseArr = [];
            foreach ($table['primaryKey'] as $index => $value) {
                $joinClauseArr[] = sprintf(
                    '%s.%s=%s.%s',
                    $targetTable,
                    $this->db->quoteIdentifier($value),
                    $sourceTable,
                    $this->db->quoteIdentifier($value)
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

    public static function getAllowedTypes(): array
    {
        return self::$allowedTypes;
    }

    public function tableExists(string $tableName): bool
    {
        $res = $this->db->fetchAll(sprintf(
            "
                SELECT *
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = %s
                AND TABLE_SCHEMA = %s
                AND TABLE_CATALOG = %s
            ",
            $this->db->quote($tableName),
            $this->db->quote($this->dbParams['schema']),
            $this->db->quote($this->dbParams['database'])
        ));


        return !empty($res);
    }

    private function execQuery($query)
    {
        $this->logger->info(sprintf("Executing query '%s'", $this->hideCredentialsInQuery($query)));
        try {
            $this->db->query($query);
        } catch (\Throwable $e) {
            throw new UserException("Query execution error: " . $e->getMessage(), 0, $e);
        }
    }

    public function showTables(string $dbName): array
    {
        throw new ApplicationException("Method not implemented");
    }

    public function getTableInfo(string $tableName): array
    {
        $sql = sprintf(
            "DESC TABLE %s;",
            $this->db->quoteIdentifier($tableName)
        );

        return $this->db->fetchAll($sql);
    }

    public function write(CsvFile $csv, array $table): void
    {
        throw new ApplicationException("Method not implemented");
    }

    public function getUserDefaultWarehouse()
    {
        $sql = sprintf(
            "DESC USER %s;",
            $this->db->quoteIdentifier($this->getCurrentUser())
        );

        $config = $this->db->fetchAll($sql);

        foreach ($config as $item) {
            if ($item['property'] === 'DEFAULT_WAREHOUSE') {
                return $item['value'] === 'null' ? null : $item['value'];
            }
        }

        return null;
    }

    public function getTimestampTypeMapping()
    {
        $sql = sprintf(
            "SHOW PARAMETERS LIKE 'TIMESTAMP_TYPE_MAPPING';",
            $this->db->quoteIdentifier($this->getCurrentUser())
        );

        $config = $this->db->fetchAll($sql);

        foreach ($config as $item) {
            if ($item['key'] === 'TIMESTAMP_TYPE_MAPPING') {
                return $item['value'];
            }
        }

        return null;
    }

    public function testConnection()
    {
        $this->execQuery('SELECT current_date;');
    }

    public function generateTmpName(string $tableName): string
    {
        $tmpId = '_temp_' . uniqid('wr_db_', true);
        return mb_substr($tableName, 0, 256 - mb_strlen($tmpId)) . $tmpId;
    }

    /**
     * Generate stage name for given run ID
     *
     * @param string $runId
     * @return string
     */
    public function generateStageName(string $runId)
    {
        return rtrim(
            mb_substr(
                sprintf(
                    "%s-%s",
                    self::STAGE_NAME,
                    str_replace('.', '-', $runId)
                ),
                0,
                255
            ),
            '-'
        );
    }

    public function getCurrentUser()
    {
        return $this->db->fetchAll("SELECT CURRENT_USER;")[0]['CURRENT_USER'];
    }

    public function checkColumns(array $columns, string $targetTable)
    {
        $columnsInDb = $this->describeTableColumns($targetTable);
        $mappingColumns = $this->describeMappingColumns($columns);


        $requiredColumnsInDb = array_keys($mappingColumns);
        $dbMissingColumns = array_values(array_udiff(array_keys($columnsInDb), $requiredColumnsInDb, 'strcasecmp'));
        if (count($dbMissingColumns) > 0) {
            throw new UserException(
                sprintf(
                    "Some columns are missing in the mapping. Missing columns: %s",
                    implode(',', $dbMissingColumns)
                )
            );
        }

        $workspaceMissingColumns = array_values(array_udiff($requiredColumnsInDb, array_keys($columnsInDb), 'strcasecmp'));
        if (count($workspaceMissingColumns) > 0) {
            throw new UserException(
                sprintf(
                    "Some columns are missing in DB table %s. Missing columns: %s",
                    $targetTable,
                    implode(',', $workspaceMissingColumns)
                )
            );
        }
    }

    public function checkDataTypes(array $columns, string $targetTable)
    {
        $timestampMapping = $this->getTimestampTypeMapping();
        if (!$timestampMapping) {
            throw new UserException("Cannot detect Snowflake TIMESTAMP_TYPE_MAPPING");
        }

        $columnsInDb = $this->describeTableColumns($targetTable);
        $mappingColumns = $this->describeMappingColumns($columns);

        //@FIXME rename test!
        $dataTypeErrors = [];
        foreach ($mappingColumns as $column => $definition) {
            if (!array_key_exists($column, $columnsInDb)) {
                throw new UserException(
                    sprintf(
                        "Some columns are missing in DB table %s. Missing columns: %s",
                        $targetTable,
                        $column
                    )
                );
            }

            $workspaceDefinition = $columnsInDb[$column];

            $invalidColumnMapping = false;
            if ($definition->getSnowflakeBaseType($timestampMapping) !== $workspaceDefinition->getType()) {
                $invalidColumnMapping = true;
            } elseif ($definition->isNullable() !== $workspaceDefinition->isNullable()) {
                $invalidColumnMapping = true;
            } elseif ($definition->getLength() !== $workspaceDefinition->getLength()) {
                if ($definition->getLength() !== null) {
                    $invalidColumnMapping = true;
                } elseif ($definition->getSnowflakeDefaultLength() !== $workspaceDefinition->getLength()) {
                    $invalidColumnMapping = true;
                }
            }

            if ($invalidColumnMapping) {
                $dataTypeErrors[$column] = [
                    $definition,
                    $workspaceDefinition,
                ];
            }
        }

        if (count($dataTypeErrors)) {
            $errorParts = [];
            foreach ($dataTypeErrors as $column => $definitions) {
                /**
                 * @var DataType\Definition $definition
                 */
                /**
                 * @var DataType\Definition $workspaceDefinition
                 */
                list($definition, $workspaceDefinition) = $definitions;

                $errorParts[] = sprintf(
                    "'%s' mapping '%s' / '%s'",
                    $column,
                    $definition->getSQLDefinition(),
                    $workspaceDefinition->getSQLDefinition()
                );
            }

            throw new UserException(
                sprintf(
                    "Different mapping between incremental load and workspace for columns: %s",
                    implode(',', $errorParts)
                )
            );
        }
    }

    /**
     * @return DataType\Definition[]
     */
    private function describeMappingColumns(array $columns): array
    {
        $return = [];
        foreach ($columns as $columnDefinition) {
            $return[$columnDefinition['dbName']] = new DataType\Definition(
                $columnDefinition['type'],
                [
                    //@FIXME default value
                    "nullable" => $columnDefinition['nullable'] ? true : false,
                    "length" => !empty($columnDefinition['size']) ? (string) $columnDefinition['size'] : null,
                ]
            );
        }

        return $return;
    }

    /**
     * @return DataType\Definition[]
     */
    private function describeTableColumns(string $targetTable): array
    {
        $return = [];

        foreach ($this->getTableInfo($targetTable) as $meta) {
            if ($meta['kind'] !== 'COLUMN') {
                continue;
            }

            $return[$meta['name']] = DataType\Definition::createFromSnowflakeMetadata($meta);
        }

        return $return;
    }

    public function checkPrimaryKey(array $columns, string $targetTable): void
    {
        $primaryKeysInDb = $this->db->getTablePrimaryKey($this->dbParams['schema'], $targetTable);

        sort($primaryKeysInDb);
        sort($columns);

        if ($primaryKeysInDb !== $columns) {
            throw new UserException(sprintf(
                'Primary key(s) in configuration does NOT match with keys in DB table.' . PHP_EOL
                . 'Keys in configuration: %s' . PHP_EOL
                . 'Keys in DB table: %s',
                implode(',', $columns),
                implode(',', $primaryKeysInDb)
            ));
        }
    }

    private function addPrimaryKeyIfMissing(array $columns, string $targetTable): void
    {
        $primaryKeysInDb = $this->db->getTablePrimaryKey($this->dbParams['schema'], $targetTable);
        if (!empty($primaryKeysInDb)) {
            return;
        }

        $sql = sprintf(
            "ALTER TABLE %s ADD %s;",
            $this->nameWithSchemaEscaped($targetTable),
            $this->getPrimaryKeySqlDefinition($columns)
        );

        $this->execQuery($sql);
    }

    private function getColumnsSqlDefinition(array $table): string
    {
        $columns = array_filter($table['items'], function ($item) {
            return (strtolower($item['type']) !== 'ignore');
        });

        $sql = '';

        foreach ($columns as $col) {
            $type = strtoupper($col['type']);
            if (!empty($col['size']) && in_array(strtolower($col['type']), self::$typesWithSize)) {
                $type .= sprintf("(%s)", $col['size']);
            }
            $null = $col['nullable'] ? 'NULL' : 'NOT NULL';
            $default = empty($col['default']) ? '' : "DEFAULT '{$col['default']}'";
            if ($type === 'TEXT') {
                $default = '';
            }
            $sql .= sprintf(
                "%s %s %s %s,",
                $this->db->quoteIdentifier($col['dbName']),
                $type,
                $null,
                $default
            );
        }

        return trim($sql, ' ,');
    }

    private function getPrimaryKeySqlDefinition(array $primaryColumns): string
    {
        $connection = $this->db;

        return sprintf(
            "PRIMARY KEY(%s)",
            implode(
                ', ',
                array_map(
                    function ($primaryColumn) use ($connection) {
                        return $connection->quoteIdentifier($primaryColumn);
                    },
                    $primaryColumns
                )
            )
        );
    }

    private function hideCredentialsInQuery($query)
    {
        return preg_replace("/(AWS_[A-Z_]*\\s=\\s.)[0-9A-Za-z\\/\\+=]*./", '${1}...\'', $query);
    }

    private function validateAndSetWarehouse()
    {
        $envWarehouse = !empty($this->dbParams['warehouse']) ? $this->dbParams['warehouse'] : null;

        $defaultWarehouse = $this->getUserDefaultWarehouse();
        if (!$defaultWarehouse && !$envWarehouse) {
            throw new UserException('Snowflake user has any "DEFAULT_WAREHOUSE" specified. Set "warehouse" parameter.');
        }

        $warehouse = $envWarehouse ?: $defaultWarehouse;

        try {
            $this->db->query(sprintf(
                'USE WAREHOUSE %s;',
                $this->db->quoteIdentifier($warehouse)
            ));
        } catch (\Throwable $e) {
            if (preg_match('/Object does not exist/ui', $e->getMessage())) {
                throw new UserException(sprintf('Invalid warehouse "%s" specified', $warehouse));
            } else {
                throw $e;
            }
        }
    }

    private function validateAndSetSchema()
    {
        try {
            $this->db->query(sprintf(
                'USE SCHEMA %s;',
                $this->db->quoteIdentifier($this->dbParams['schema'])
            ));
        } catch (\Throwable $e) {
            if (preg_match('/Object does not exist/ui', $e->getMessage())) {
                throw new UserException(sprintf('Invalid schema "%s" specified', $this->dbParams['schema']));
            } else {
                throw $e;
            }
        }
    }

    public function validateTable(array $tableConfig): void
    {
        $this->checkPrimaryKey($tableConfig['primaryKey'], $tableConfig['dbName']);
        $this->checkColumns($tableConfig['items'], $tableConfig['dbName']);
        $this->checkDataTypes($tableConfig['items'], $tableConfig['dbName']);
    }

    public function getConnection(): \PDO
    {
        throw new ApplicationException("Method not implemented");
    }

    public function getSnowflakeConnection(): Connection
    {
        return $this->db;
    }
}
