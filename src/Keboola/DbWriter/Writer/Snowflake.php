<?php

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Adapter\IAdapter;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Monolog\Logger;
use Keboola\DbWriter\Snowflake\Connection;
use Keboola\DbWriter\Writer;
use Keboola\DbWriter\WriterInterface;

class Snowflake extends Writer implements WriterInterface
{
    public const WRITER = 'Snowflake';
    private const STATEMENT_TIMEOUT_IN_SECONDS = 10800;
    public const STAGE_NAME = 'db-writer';

    /** @var string[]  */
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

    /** @var string[]  */
    private static $typesWithSize = [
        'number', 'decimal', 'numeric',
        'char', 'character', 'varchar', 'string', 'text', 'binary',
    ];

    /** @var Connection */
    protected $db;

    /** @var Logger */
    protected $logger;

    protected IAdapter $adapter;

    public function __construct(array $dbParams, Logger $logger, IAdapter $adapter)
    {
        $this->logger = $logger;
        $this->dbParams = $dbParams;
        $this->adapter = $adapter;

        try {
            $this->db = $this->createSnowflakeConnection($this->dbParams);
        } catch (\Throwable $e) {
            if (strstr(strtolower($e->getMessage()), 'could not find driver')) {
                throw new ApplicationException('Missing driver: ' . $e->getMessage());
            }
            throw new UserException('Error connecting to DB: ' . $e->getMessage(), 0, $e);
        }

        $this->validateAndSetWarehouse();
        $this->validateAndSetSchema();
    }

    public function createConnection(array $dbParams): \PDO
    {
        throw new ApplicationException('Method not implemented');
    }

    public function createSnowflakeConnection(array $dbParams): Connection
    {
        $runId = $dbParams['runId'] ?? getenv('KBC_RUNID');
        if ($runId) {
            $dbParams['runId'] = $runId;
        }

        $connection = new Connection($dbParams);
        $connection->query(sprintf('ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = %d', self::STATEMENT_TIMEOUT_IN_SECONDS));
        return $connection;
    }

    public function writeFromAdapter(array $table): void
    {
        $this->execQuery($this->generateDropStageCommand()); // remove old db wr stage

        $stageName = $this->generateStageName((string) getenv('KBC_RUNID'));

        $this->execQuery($this->adapter->generateCreateStageCommand($stageName));

        try {
            $this->execQuery(
                $this->adapter->generateCopyCommand(
                    $this->nameWithSchemaEscaped($table['dbName']),
                    $stageName,
                    $table['items']
                )
            );
        } catch (UserException $e) {
            $this->execQuery($this->generateDropStageCommand($stageName));
            throw $e;
        }

        $this->execQuery($this->generateDropStageCommand($stageName));
    }

    private function generateDropStageCommand(string $stage = self::STAGE_NAME): string
    {
        return sprintf(
            'DROP STAGE IF EXISTS %s',
            $this->quoteIdentifier($stage)
        );
    }

    protected function nameWithSchemaEscaped(string $tableName, ?string $schemaName = null): string
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

    public static function quote(string $value): string
    {
        return "'" . addslashes($value) . "'";
    }

    public static function quoteIdentifier(string $value): string
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }

    public function drop(string $tableName): void
    {
        $this->execQuery(sprintf('DROP TABLE IF EXISTS %s;', $this->quoteIdentifier($tableName)));
    }

    public function create(array $table): void
    {
        $sqlDefinitions = [$this->getColumnsSqlDefinition($table)];
        if (!empty($table['primaryKey'])) {
            $sqlDefinitions [] = $this->getPrimaryKeySqlDefinition($table['primaryKey']);
        }

        $this->execQuery(sprintf(
            'CREATE TABLE %s (%s);',
            $this->quoteIdentifier($table['dbName']),
            implode(', ', $sqlDefinitions)
        ));
    }

    public function createIfNotExists(array $table): void
    {
        $sqlDefinitions = [$this->getColumnsSqlDefinition($table)];
        if (!empty($table['primaryKey'])) {
            $sqlDefinitions [] = $this->getPrimaryKeySqlDefinition($table['primaryKey']);
        }

        $this->execQuery(sprintf(
            'CREATE TABLE IF NOT EXISTS %s (%s);',
            $this->quoteIdentifier($table['dbName']),
            implode(', ', $sqlDefinitions)
        ));
    }

    public function swapTables(string $table1, string $table2): void
    {
        $this->execQuery(sprintf(
            'ALTER TABLE %s SWAP WITH %s',
            $this->quoteIdentifier($table2),
            $this->quoteIdentifier($table1)
        ));
    }

    public function createStaging(array $table): void
    {
        $sqlDefinitions = [$this->getColumnsSqlDefinition($table)];
        if (!empty($table['primaryKey'])) {
            $sqlDefinitions [] = $this->getPrimaryKeySqlDefinition($table['primaryKey']);
        }

        $this->execQuery(sprintf(
            'CREATE TEMPORARY TABLE %s (%s);',
            $this->quoteIdentifier($table['dbName']),
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
                return $this->quoteIdentifier($item['dbName']);
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
                'UPDATE %s SET %s FROM %s WHERE %s',
                $targetTable,
                $valuesClause,
                $sourceTable,
                $joinClause
            ));

            // delete updated from temp table
            $this->execQuery(sprintf(
                'DELETE FROM %s USING %s WHERE %s',
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
            '
                SELECT *
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = %s
                AND TABLE_SCHEMA = %s
                AND TABLE_CATALOG = %s
            ',
            $this->quote($tableName),
            $this->quote($this->dbParams['schema']),
            $this->quote($this->dbParams['database'])
        ));

        return !empty($res);
    }

    public function createForeignKeys(array $table): void
    {
        foreach ($table['items'] as $column) {
            if (empty($column['foreignKeyTable']) || empty($column['foreignKeyColumn'])) {
                continue;
            }

            if (!$this->tableExists($column['foreignKeyTable'])) {
                continue;
            }

            if (!$this->isSameTypeColumns($table['dbName'], $column['dbName'], $column['foreignKeyTable'], $column['foreignKeyColumn'])) {
                throw new UserException(sprintf(
                    'Foreign key column \'%s\' in table \'%s\' must be the same type as column \'%s\' in source table',
                    $column['foreignKeyColumn'],
                    $column['foreignKeyTable'],
                    $column['name']
                ));
            }

            $this->addUniqueKeyIfMissing($column['foreignKeyTable'], $column['foreignKeyColumn']);

            $this->execQuery(sprintf(
                'ALTER TABLE %s ADD CONSTRAINT FK_%s_%s FOREIGN KEY (%s) REFERENCES %s(%s)',
                $this->nameWithSchemaEscaped($table['dbName']),
                $column['foreignKeyTable'],
                $column['foreignKeyColumn'],
                $this->quoteIdentifier($column['dbName']),
                $this->nameWithSchemaEscaped($column['foreignKeyTable']),
                $this->quoteIdentifier($column['foreignKeyColumn'])
            ));
        }
    }

    private function execQuery(string $query): void
    {
        $this->logger->info(sprintf("Executing query '%s'", $this->hideCredentialsInQuery($query)));
        try {
            $this->db->query($query);
        } catch (\Throwable $e) {
            throw new UserException('Query execution error: ' . $e->getMessage(), 0, $e);
        }
    }

    public function showTables(string $dbName): array
    {
        throw new ApplicationException('Method not implemented');
    }

    public function getTableInfo(string $tableName): array
    {
        throw new ApplicationException('Method not implemented');
    }

    public function write(CsvFile $csv, array $table): void
    {
        throw new ApplicationException('Method not implemented');
    }

    public function getUserDefaultWarehouse(): ?string
    {
        $sql = sprintf(
            'DESC USER %s;',
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

    public function testConnection(): void
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
                    '%s-%s',
                    self::STAGE_NAME,
                    str_replace('.', '-', $runId)
                ),
                0,
                255
            ),
            '-'
        );
    }

    public function getCurrentUser(): string
    {
        return $this->db->fetchAll('SELECT CURRENT_USER;')[0]['CURRENT_USER'];
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

    public function checkForeignKey(string $sourceTable, string $targetTable, string $targetColumn): void
    {
        $foreignKeys = $this->db->getTableConstraints($this->dbParams['schema'], $sourceTable);

        $constraint = array_filter($foreignKeys, function ($item) use ($targetTable, $targetColumn) {
            $constraintName = sprintf('FK_%s_%s', strtoupper($targetTable), strtoupper($targetColumn));
            if ($item['CONSTRAINT_NAME'] === $constraintName) {
                return true;
            }
            return false;
        });

        if (!$constraint) {
            throw new UserException(sprintf('Foreign keys on table  \'%s\' does not exists', $sourceTable));
        }
    }

    private function addPrimaryKeyIfMissing(array $columns, string $targetTable): void
    {
        $primaryKeysInDb = $this->db->getTablePrimaryKey($this->dbParams['schema'], $targetTable);
        if (!empty($primaryKeysInDb)) {
            return;
        }

        $sql = sprintf(
            'ALTER TABLE %s ADD %s;',
            $this->nameWithSchemaEscaped($targetTable),
            $this->getPrimaryKeySqlDefinition($columns)
        );

        $this->execQuery($sql);
    }

    private function addUniqueKeyIfMissing(string $targetTable, string $targetColumn): void
    {
        $uniquesInDb = $this->db->getTableUniqueKeys($this->dbParams['schema'], $targetTable);
        $primaryKeysInDb = $this->db->getTablePrimaryKey($this->dbParams['schema'], $targetTable);
        if (in_array($targetColumn, $uniquesInDb) || !empty($primaryKeysInDb)) {
            return;
        }

        $this->execQuery(sprintf(
            'ALTER TABLE %s ADD UNIQUE (%s);',
            $this->nameWithSchemaEscaped($targetTable),
            $this->quoteIdentifier($targetColumn)
        ));
    }

    private function isSameTypeColumns(string $sourceTable, string $sourceColumnName, string $targetTable, string $targetColumnName): bool
    {
        $sourceColumnDataType = $this->db->getColumnDataType(
            $this->dbParams['schema'],
            $sourceTable,
            $sourceColumnName
        );

        $targetColumnDataType = $this->db->getColumnDataType(
            $this->dbParams['schema'],
            $targetTable,
            $targetColumnName
        );

        return
            $sourceColumnDataType->type === $targetColumnDataType->type &&
            $sourceColumnDataType->length === $targetColumnDataType->length &&
            $sourceColumnDataType->nullable === $targetColumnDataType->nullable;
    }

    private function getColumnsSqlDefinition(array $table): string
    {
        $columns = array_filter((array) $table['items'], function ($item) {
            return (strtolower($item['type']) !== 'ignore');
        });

        $sql = '';

        foreach ($columns as $col) {
            $type = strtoupper($col['type']);
            if (!empty($col['size']) && in_array(strtolower($col['type']), self::$typesWithSize)) {
                $type .= sprintf('(%s)', $col['size']);
            }
            $null = $col['nullable'] ? 'NULL' : 'NOT NULL';
            $default = empty($col['default']) ? '' : "DEFAULT '{$col['default']}'";
            if ($type === 'TEXT') {
                $default = '';
            }
            $sql .= sprintf(
                '%s %s %s %s,',
                $this->quoteIdentifier($col['dbName']),
                $type,
                $null,
                $default
            );
        }

        return trim($sql, ' ,');
    }

    private function getPrimaryKeySqlDefinition(array $primaryColumns): string
    {
        $writer = $this;

        return sprintf(
            'PRIMARY KEY(%s)',
            implode(
                ', ',
                array_map(
                    function ($primaryColumn) use ($writer) {
                        return $writer->quoteIdentifier($primaryColumn);
                    },
                    $primaryColumns
                )
            )
        );
    }

    private function hideCredentialsInQuery(string $query): ?string
    {
        return preg_replace(
            '/(AZURE_[A-Z_]*\\s=\\s.|AWS_[A-Z_]*\\s=\\s.)[0-9A-Za-z\\/\\+=\\-&:%]*/',
            '${1}...\'',
            $query
        );
    }

    private function validateAndSetWarehouse(): void
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

    private function validateAndSetSchema(): void
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
        // TODO: Implement validateTable() method.
    }

    public function getConnection(): \PDO
    {
        throw new ApplicationException('Method not implemented');
    }

    public function getSnowflakeConnection(): Connection
    {
        return $this->db;
    }
}
