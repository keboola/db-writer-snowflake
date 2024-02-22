<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriter\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbWriter\Configuration\ValueObject\SnowflakeItemConfig;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Writer\Strategy\AbsWriteStrategy;
use Keboola\DbWriter\Writer\Strategy\S3WriteStrategy;
use Keboola\DbWriter\Writer\Strategy\WriteStrategy;
use Keboola\DbWriterAdapter\ODBC\OdbcWriteAdapter;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;

/**
 * @property-read SnowflakeQueryBuilder $queryBuilder
 */
class SnowflakeWriteAdapter extends OdbcWriteAdapter
{
    public function writeData(string $tableName, ExportConfig $exportConfig): void
    {
        $this->logger->info(sprintf('Writing data to table "%s"', $tableName));

        /** @var SnowflakeDatabaseConfig $databaseConfig */
        $databaseConfig = $exportConfig->getDatabaseConfig();

        $stageName = $this->generateStageName($databaseConfig->hasRunId() ? $databaseConfig->getRunId() : '');

        $this->logger->info(sprintf('Dropping stage "%s"', $stageName));
        $this->connection->exec($this->queryBuilder->dropStageStatement($this->connection, $stageName));

        $writeStrategy = $this->getTableWriteStrategy($exportConfig->getTableFilePath());

        $this->logger->info(sprintf('Creating stage "%s"', $stageName));
        $this->connection->exec($writeStrategy->generateCreateStageCommand($stageName));

        $tableNameWithSchema = sprintf(
            '%s.%s',
            $this->connection->quoteIdentifier($databaseConfig->getSchema()),
            $this->connection->quoteIdentifier($tableName),
        );
        try {
            $items = array_filter(
                $exportConfig->getItems(),
                fn(ItemConfig $item) => strtolower($item->getType()) !== 'ignore',
            );
            $commands = $writeStrategy->generateCopyCommands(
                tableName: $tableNameWithSchema,
                stageName: $stageName,
                items: $items,
            );
            foreach ($commands as $command) {
                $this->connection->exec($command);
            }
        } finally {
            $this->connection->exec($this->queryBuilder->dropStageStatement($this->connection, $stageName));
        }
    }

    public function upsert(ExportConfig $exportConfig, string $stageTableName): void
    {
        $this->logger->info(sprintf('Upserting data to table "%s"', $exportConfig->getDbName()));
        if ($exportConfig->hasPrimaryKey()) {
            $this->addPrimaryKeyIfMissing($exportConfig->getPrimaryKey(), $exportConfig->getDbName());
            $this->checkPrimaryKey($exportConfig->getPrimaryKey(), $exportConfig->getDbName());
        }

        parent::upsert($exportConfig, $stageTableName);
    }

    public function swapTable(SnowflakeConnection $connection, string $tableName, string $stagingTableName): void
    {
        $this->logger->info(sprintf('Swapping table "%s" with "%s"', $stagingTableName, $tableName));
        $connection->exec(sprintf(
            'ALTER TABLE %s SWAP WITH %s',
            $this->connection->quoteIdentifier($stagingTableName),
            $this->connection->quoteIdentifier($tableName),
        ));
    }

    /**
     * @return array{Field: string, Type: string}[]
     */
    public function getTableInfo(string $tableName): array
    {
        /** @var array{name: string, type: string}[] $res */
        $res = $this->connection->fetchAll(
            $this->queryBuilder->tableInfoQueryStatement($this->connection, $tableName),
        );

        return array_map(fn(array $item) => [
            'Field' => (string) $item['name'],
            'Type' => (string) $item['type'],
        ], $res);
    }

    public function validateTable(string $tableName, array $items): void
    {
        // turn off validation
    }

    public function isSameTypeColumns(
        string $sourceTable,
        string $sourceColumnName,
        string $targetTable,
        string $targetColumnName,
    ): bool {
        $sourceColumnDataType = $this->getColumnDataType(
            $sourceTable,
            $sourceColumnName,
        );

        $targetColumnDataType = $this->getColumnDataType(
            $targetTable,
            $targetColumnName,
        );

        return
            $sourceColumnDataType['type'] === $targetColumnDataType['type'] &&
            $sourceColumnDataType['length'] === $targetColumnDataType['length'] &&
            $sourceColumnDataType['nullable'] === $targetColumnDataType['nullable'];
    }

    public function addUniqueKeyIfMissing(string $targetTable, string $targetColumn): void
    {
        $this->logger->info(sprintf(
            'Adding unique key to table "%s" on column "%s"',
            $targetTable,
            $targetColumn,
        ));
        $tableInfo = $this->connection->fetchAll(
            $this->queryBuilder->tableInfoQueryStatement($this->connection, $targetTable),
        );

        $uniquesInDb = array_filter($tableInfo, fn($v) => $v['unique key'] === 'Y');
        $uniquesInDb = array_map(fn(array $item) => $item['name'], $uniquesInDb);

        $primaryKeysInDb = $this->getPrimaryKeys($targetTable);
        $primaryKeysInDb = array_map(fn(array $item) => $item['name'], $primaryKeysInDb);

        if (in_array($targetColumn, $uniquesInDb) || !empty($primaryKeysInDb)) {
            return;
        }

        $this->connection->exec(
            $this->queryBuilder->addUniqueKeyQueryStatement($this->connection, $targetTable, $targetColumn),
        );
    }

    public function addForeignKey(string $targetTable, SnowflakeItemConfig $item): void
    {
        $this->logger->info(sprintf(
            'Creating foreign key from table "%s" to table "%s" on column "%s"',
            $item->getDbName(),
            $item->getForeignKeyTable(),
            $item->getForeignKeyColumn(),
        ));
        $this->connection->exec(
            $this->queryBuilder->addForeignKeyQueryStatement(
                $this->connection,
                $targetTable,
                $item->getDbName(),
                $item->getForeignKeyTable(),
                $item->getForeignKeyColumn(),
            ),
        );
    }

    public function getPrimaryKeys(string $tableName): array
    {
        $sqlPrimaryKeysInDb = $this->connection->fetchAll(
            $this->queryBuilder->tableInfoQueryStatement($this->connection, $tableName),
        );
        return array_filter($sqlPrimaryKeysInDb, fn($v) => $v['primary key'] === 'Y');
    }

    private function getTableWriteStrategy(string $getTableFilePath): WriteStrategy
    {
        /**
         * @var array{s3?: array, abs?: array} $manifest
         */
        $manifest = json_decode(
            (string) file_get_contents($getTableFilePath . '.manifest'),
            true,
        );

        if (isset($manifest[WriteStrategy::FILE_STORAGE_S3])) {
            $this->logger->info('Using S3 write strategy');
            return new S3WriteStrategy($manifest[WriteStrategy::FILE_STORAGE_S3]);
        }
        if (isset($manifest[WriteStrategy::FILE_STORAGE_ABS])) {
            $this->logger->info('Using ABS write strategy');
            return new AbsWriteStrategy($manifest[WriteStrategy::FILE_STORAGE_ABS]);
        }
        throw new UserException('Unknown input adapter');
    }

    private function generateStageName(string $runId): string
    {
        $stageName = sprintf(
            'db-writer-%s',
            str_replace('.', '-', $runId),
        );

        return rtrim(mb_substr($stageName, 0, 255), '-');
    }

    private function addPrimaryKeyIfMissing(array $primaryKeys, string $tableName): void
    {
        $primaryKeysInDb = $this->getPrimaryKeys($tableName);

        if (!empty($primaryKeysInDb)) {
            return;
        }

        $this->connection->exec(
            $this->queryBuilder->addPrimaryKeyQueryStatement($this->connection, $tableName, $primaryKeys),
        );
    }

    private function checkPrimaryKey(array $primaryKeys, string $tableName): void
    {
        $primaryKeysInDb = $this->getPrimaryKeys($tableName);
        $primaryKeysInDb = array_map(fn(array $item) => $item['name'], $primaryKeysInDb);

        sort($primaryKeysInDb);
        sort($primaryKeys);

        if ($primaryKeysInDb !== $primaryKeys) {
            throw new UserException(sprintf(
                'Primary key(s) in configuration does NOT match with keys in DB table.' . PHP_EOL
                . 'Keys in configuration: %s' . PHP_EOL
                . 'Keys in DB table: %s',
                implode(',', $primaryKeys),
                implode(',', $primaryKeysInDb),
            ));
        }
    }

    private function getColumnDataType(string $table, string $column): array
    {
        $columns = $this->connection->fetchAll(
            $this->queryBuilder->describeTableColumnsQueryStatement($this->connection, $table),
        );
        /**
         * @var array{column_name: string, data_type: string}[] $columnData
         */
        $columnData = array_values(array_filter($columns, fn($v) => $v['column_name'] === $column));

        if (count($columnData) === 0) {
            throw new UserException(sprintf('Column \'%s\' in table \'%s\' not found', $column, $table));
        }

        return (array) json_decode($columnData[0]['data_type'], true);
    }
}
