<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\DbWriter\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbWriter\Configuration\ValueObject\SnowflakeExportConfig;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Writer\Snowflake;
use Keboola\DbWriter\Writer\SnowflakeConnection;
use Keboola\DbWriter\Writer\SnowflakeConnectionFactory;
use Keboola\DbWriter\Writer\SnowflakeQueryBuilder;
use Keboola\DbWriter\Writer\SnowflakeWriteAdapter;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use Psr\Log\Test\TestLogger;

class SnowflakeTest extends TestCase
{
    private LoggerInterface $logger;

    public function setUp(): void
    {
        $this->logger = new TestLogger();
        $this->dropAllTables();
    }

    public function testTmpName(): void
    {
        $adapter = $this->getWriteAdapter($this->getConfig('simple'));

        $tableName = 'firstTable';

        $tmpName = $adapter->generateTmpName($tableName);
        $this->assertMatchesRegularExpression('/' . $tableName . '/ui', $tmpName);
        $this->assertMatchesRegularExpression('/temp/ui', $tmpName);
        $this->assertLessThanOrEqual(256, mb_strlen($tmpName));

        $tableName = str_repeat('firstTableWithLongName', 15);

        $this->assertGreaterThanOrEqual(256, mb_strlen($tableName));
        $tmpName = $adapter->generateTmpName($tableName);
        $this->assertMatchesRegularExpression('/temp/ui', $tmpName);
        $this->assertLessThanOrEqual(256, mb_strlen($tmpName));
    }

    public function testCreateAndDropTable(): void
    {
        $table = 'simple';

        $config = $this->getConfig($table);
        $exportConfig = $this->getExportConfig($config);
        $adapter = $this->getWriteAdapter($config);
        $connection = $this->getConnection($config);

        $adapter->create($exportConfig->getDbName(), false, $exportConfig->getItems());

        Assert::assertTrue($adapter->tableExists($exportConfig->getDbName()));

        // check table type
        $tablesInfo = $connection->fetchAll(sprintf(
            "SHOW TABLES LIKE '%s';",
            $exportConfig->getDbName(),
        ));

        Assert::assertCount(1, $tablesInfo);

        /** @var array{schema_name: string, database_name: string, name: string, kind: string} $tableInfo */
        $tableInfo = reset($tablesInfo);
        Assert::assertEquals($exportConfig->getDatabaseConfig()->getSchema(), $tableInfo['schema_name']);
        Assert::assertEquals($exportConfig->getDatabaseConfig()->getDatabase(), $tableInfo['database_name']);
        Assert::assertEquals($exportConfig->getDbName(), $tableInfo['name']);
        Assert::assertEquals('TRANSIENT', $tableInfo['kind']);

        $adapter->drop($exportConfig->getDbName());

        Assert::assertFalse($adapter->tableExists($exportConfig->getDbName()));
    }

    public function createStagingData(): array
    {
        return [
            [true, 'TEMPORARY'],
            [false, 'TEMPORARY'],
        ];
    }

    /**
     * @dataProvider createStagingData
     */
    public function testCreateStaging(bool $incrementalValue, string $expectedKind): void
    {
        $config = $this->getConfig('simple');
        $config['parameters']['incremental'] = $incrementalValue;

        $exportConfig = $this->getExportConfig($config);
        $connection = $this->getConnection($config);
        $adapter = $this->getWriteAdapter($config, $connection);

        if ($adapter->tableExists($exportConfig->getDbName())) {
            $adapter->drop($exportConfig->getDbName());
        }

        Assert::assertFalse($adapter->tableExists($exportConfig->getDbName()));

        $adapter->create($exportConfig->getDbName(), true, $exportConfig->getItems());

        Assert::assertTrue($adapter->tableExists($exportConfig->getDbName()));

        // check table type
        $tablesInfo = $connection->fetchAll(sprintf(
            "SHOW TABLES LIKE '%s';",
            $exportConfig->getDbName(),
        ));

        Assert::assertCount(1, $tablesInfo);

        /** @var array{schema_name: string, database_name: string, name: string, kind: string} $tableInfo */
        $tableInfo = reset($tablesInfo);
        Assert::assertEquals($exportConfig->getDatabaseConfig()->getSchema(), $tableInfo['schema_name']);
        Assert::assertEquals($exportConfig->getDatabaseConfig()->getDatabase(), $tableInfo['database_name']);
        Assert::assertEquals($exportConfig->getDbName(), $tableInfo['name']);
        Assert::assertEquals($expectedKind, $tableInfo['kind']);
    }

    public function testSwap(): void
    {
        $table1 = $this->getConfig('simple');
        $table2 = $this->getConfig('special');

        $connection = $this->getConnection($table1);

        $exportConfig1 = $this->getExportConfig($table1);
        $exportConfig2 = $this->getExportConfig($table2);

        $adapter1 = $this->getWriteAdapter($table1);
        $adapter2 = $this->getWriteAdapter($table2);

        $adapter1->create($exportConfig1->getDbName(), false, $exportConfig1->getItems());
        $adapter2->create($exportConfig2->getDbName(), false, $exportConfig2->getItems());

        $table1Columns = $connection->fetchAll("DESCRIBE TABLE \"{$exportConfig1->getDbName()}\"");
        $table2Columns = $connection->fetchAll("DESCRIBE TABLE \"{$exportConfig2->getDbName()}\"");

        $adapter1->swapTable($connection, $exportConfig1->getDbName(), $exportConfig2->getDbName());

        $tableSwap1Columns = $connection->fetchAll("DESCRIBE TABLE \"{$exportConfig1->getDbName()}\"");
        $tableSwap2Columns = $connection->fetchAll("DESCRIBE TABLE \"{$exportConfig2->getDbName()}\"");

        Assert::assertEquals($table1Columns, $tableSwap2Columns);
        Assert::assertEquals($table2Columns, $tableSwap1Columns);
    }

    public function testDefaultWarehouse(): void
    {
        $config = $this->getConfig('simple');
        $connection = $this->getConnection($config);
        /** @var array{'CURRENT_USER': string}[] $currentUser */
        $currentUser = $connection->fetchAll('SELECT CURRENT_USER;');
        $user = (string) $currentUser[0]['CURRENT_USER'];

        $warehouse = $config['parameters']['db']['warehouse'];

        // run without warehouse param
        unset($config['parameters']['db']['warehouse']);
        $this->setUserDefaultWarehouse($connection, $user, null);
        Assert::assertEmpty($this->getUserDefaultWarehouse($connection, $user));
        /** @var SnowflakeDatabaseConfig $databaseConfig */
        $databaseConfig = $this->getExportConfig($config)->getDatabaseConfig();

        try {
            new Snowflake($databaseConfig, $this->logger);
            $this->fail('Create writer without warehouse should fail');
        } catch (UserException $e) {
            $this->assertMatchesRegularExpression(
                '/Snowflake user has any \"DEFAULT_WAREHOUSE\" specified/ui',
                $e->getMessage(),
            );
        }

        // run with warehouse param
        $config['parameters']['db']['warehouse'] = $warehouse;
        /** @var SnowflakeDatabaseConfig $databaseConfig */
        $databaseConfig = $this->getExportConfig($config)->getDatabaseConfig();
        $writer = new Snowflake($databaseConfig, $this->logger);

        $writer->testConnection();

        // restore default warehouse
        $this->setUserDefaultWarehouse($connection, $user, $warehouse);
        $this->assertEquals($warehouse, $this->getUserDefaultWarehouse($connection, $user));
    }

    public function testInvalidWarehouse(): void
    {
        $config = $this->getConfig('simple');
        $config['parameters']['db']['warehouse'] = uniqid();
        /** @var SnowflakeDatabaseConfig $databaseConfig */
        $databaseConfig = $this->getExportConfig($config)->getDatabaseConfig();

        try {
            new Snowflake($databaseConfig, $this->logger);
            $this->fail('Creating connection should fail with UserError');
        } catch (UserException $e) {
            $this->assertStringContainsString('Invalid warehouse', $e->getMessage());
        }
    }

    public function testInvalidSchema(): void
    {
        $config = $this->getConfig('simple');
        $config['parameters']['db']['schema'] = uniqid();
        /** @var SnowflakeDatabaseConfig $databaseConfig */
        $databaseConfig = $this->getExportConfig($config)->getDatabaseConfig();

        try {
            new Snowflake($databaseConfig, $this->logger);
            $this->fail('Creating connection should fail with UserError');
        } catch (UserException $e) {
            $this->assertStringContainsString('Invalid schema', $e->getMessage());
        }
    }

    public function testCheckPrimaryKey(): void
    {
        $config = $this->getConfig('simple');
        $config['parameters']['primaryKey'] = ['id', 'name'];
        $adapter = $this->getWriteAdapter($config);
        $exportConfig = $this->getExportConfig($config);

        $adapter->create(
            $exportConfig->getDbName(),
            false,
            $exportConfig->getItems(),
            $exportConfig->getPrimaryKey(),
        );

        $primaryKeys = $adapter->getPrimaryKeys($exportConfig->getDbName());
        $primaryKeysName = array_map(
            fn(array $row) => $row['name'],
            $primaryKeys,
        );
        Assert::assertCount(2, $primaryKeys);
        Assert::assertEquals(['id', 'name'], $primaryKeysName);
    }

    public function testUpsertAddMissingPrimaryKey(): void
    {
        $config = $this->getConfig('simple');
        $config['parameters']['primaryKey'] = ['id', 'name'];
        $adapter = $this->getWriteAdapter($config);
        $exportConfig = $this->getExportConfig($config);

        $tmpName = $adapter->generateTmpName($config['parameters']['dbName']);

        $adapter->create($tmpName, true, $exportConfig->getItems());
        $adapter->create($exportConfig->getDbName(), false, $exportConfig->getItems());

        $primaryKeys = $adapter->getPrimaryKeys($exportConfig->getDbName());
        Assert::assertCount(0, $primaryKeys);

        $adapter->upsert($exportConfig, $tmpName);

        $primaryKeys = $adapter->getPrimaryKeys($exportConfig->getDbName());
        Assert::assertCount(2, $primaryKeys);
    }

    public function testUpsertCheckPrimaryKeyError(): void
    {
        $config = $this->getConfig('simple');
        $config['parameters']['primaryKey'] = ['id', 'name'];
        $adapter = $this->getWriteAdapter($config);
        $exportConfig = $this->getExportConfig($config);

        $tmpName = $adapter->generateTmpName($config['parameters']['dbName']);

        $adapter->create($tmpName, true, $exportConfig->getItems(), ['id']);
        $adapter->create($exportConfig->getDbName(), false, $exportConfig->getItems(), ['id']);

        try {
            $adapter->upsert($exportConfig, $tmpName);
            $this->fail('Primary key check should fail');
        } catch (UserException $e) {
            $this->assertStringContainsString(
                'Primary key(s) in configuration does NOT match with keys in DB table.',
                $e->getMessage(),
            );
        } finally {
            $adapter->drop($tmpName);
            $adapter->drop($exportConfig->getDbName());
        }
    }

    /**
     * @dataProvider queryTaggingProvider
     */
    public function testQueryTagging(array $additionalDbConfig, string $expectedRunId): void
    {
        $config = $this->getConfig('simple');
        $config['parameters']['db'] = array_merge($config['parameters']['db'], $additionalDbConfig);

        $connection = $this->getConnection($config);
        $connection->fetchAll('SELECT current_date;');

        $queries = $connection->fetchAll(
            '
                SELECT 
                    QUERY_TEXT, QUERY_TAG 
                FROM 
                    TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION())
                WHERE QUERY_TEXT = \'SELECT current_date;\' 
                ORDER BY START_TIME DESC 
                LIMIT 1
            ',
        );

        $runId = sprintf('{"runId":"%s"}', $expectedRunId);

        Assert::assertEquals($runId, $queries[0]['QUERY_TAG']);
    }

    public function queryTaggingProvider(): array
    {
        return [
            [
                [],
                getenv('KBC_RUNID'),
            ],
            [
                ['runId' => '123456'],
                '123456',
            ],
        ];
    }

    private function setUserDefaultWarehouse(
        SnowflakeConnection $connection,
        string $username,
        ?string $warehouse = null,
    ): void {
        if ($warehouse) {
            $sql = sprintf(
                'ALTER USER %s SET DEFAULT_WAREHOUSE = %s;',
                $connection->quoteIdentifier($username),
                $connection->quoteIdentifier($warehouse),
            );
            $connection->exec($sql);

            Assert::assertEquals($warehouse, $this->getUserDefaultWarehouse($connection, $username));
        } else {
            $sql = sprintf(
                'ALTER USER %s SET DEFAULT_WAREHOUSE = null;',
                $connection->quoteIdentifier($username),
            );
            $connection->exec($sql);

            Assert::assertEmpty($this->getUserDefaultWarehouse($connection, $username));
        }
    }

    private function getUserDefaultWarehouse(SnowflakeConnection $connection, string $username): ?string
    {
        $sql = sprintf(
            'DESC USER %s;',
            $connection->quoteIdentifier($username),
        );

        $config = $connection->fetchAll($sql);

        /**
         * @var array{'value': string}[] $defaultWarehouse
         */
        $defaultWarehouse = array_values(
            array_filter($config, fn ($item) => $item['property'] === 'DEFAULT_WAREHOUSE'),
        );

        if (count($defaultWarehouse) !== 1) {
            return null;
        }

        return $defaultWarehouse[0]['value'] === 'null' ? null : $defaultWarehouse[0]['value'];
    }

    private function dropAllTables(): void
    {
        $config = $this->getConfig('simple');
        $exportConfig = $this->getExportConfig($config);
        $connection = $this->getConnection($config);

        $tables = $connection->fetchAll(
            sprintf(
                'SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = \'%s\';',
                $exportConfig->getDatabaseConfig()->getSchema(),
            ),
        );

        /** @var string[] $tables */
        $tables = array_map(function ($item) {
            return $item['TABLE_NAME'];
        }, $tables);

        foreach ($tables as $tableName) {
            $connection->exec(sprintf(
                'DROP TABLE IF EXISTS %s.%s',
                $connection->quoteIdentifier($exportConfig->getDatabaseConfig()->getSchema()),
                $connection->quoteIdentifier($tableName),
            ));
        }
    }

    private function getWriteAdapter(array $config, ?SnowflakeConnection $connection = null): SnowflakeWriteAdapter
    {
        /** @var SnowflakeDatabaseConfig $databaseConfig */
        $databaseConfig = $this->getExportConfig($config)->getDatabaseConfig();

        return new SnowflakeWriteAdapter(
            $connection ?? $this->getConnection($config),
            new SnowflakeQueryBuilder($databaseConfig),
            $this->logger,
        );
    }

    private function getConnection(array $config): SnowflakeConnection
    {
        $snowflakeConnectionFactory = new SnowflakeConnectionFactory();
        /** @var SnowflakeDatabaseConfig $databaseConfig */
        $databaseConfig = $this->getExportConfig($config)->getDatabaseConfig();

        $connection = $snowflakeConnectionFactory->create(
            $databaseConfig,
            $this->logger,
        );
        $connection->exec(sprintf(
            'USE WAREHOUSE %s;',
            $connection->quoteIdentifier($databaseConfig->getWarehouse()),
        ));

        return $connection;
    }

    private function getExportConfig(array $config): SnowflakeExportConfig
    {
        return SnowflakeExportConfig::fromArray(
            $config['parameters'],
            $config['storage'],
            SnowflakeDatabaseConfig::fromArray($config['parameters']['db']),
        );
    }

    private function getConfig(string $table): array
    {
        $tableConfig = (string) file_get_contents(sprintf(
            '%s/configs/%s.json',
            __DIR__,
            $table,
        ));

        /** @var array $config */
        $config = json_decode($tableConfig, true);
        $config['parameters']['db'] = [
            'host' => (string) getenv('DB_HOST'),
            'port' => (string) getenv('DB_PORT'),
            'database' => (string) getenv('DB_DATABASE'),
            'schema' => (string) getenv('DB_SCHEMA'),
            'user' => (string) getenv('DB_USER'),
            '#password' => (string) getenv('DB_PASSWORD'),
            'warehouse' => (string) getenv('DB_WAREHOUSE'),
        ];

        return $config;
    }
}
