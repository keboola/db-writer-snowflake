<?php

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Adapter\AbsAdapter;
use Keboola\DbWriter\Adapter\IAdapter;
use Keboola\DbWriter\Adapter\S3Adapter;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Snowflake\Connection;
use Keboola\DbWriter\Snowflake\SnowflakeWriterFactory;
use Keboola\DbWriter\Snowflake\Test\StagingStorageLoader;
use Keboola\DbWriter\Writer\Snowflake;
use Keboola\StorageApi\Client;
use Monolog\Handler\TestHandler;

class SnowflakeTest extends BaseTest
{
    protected $dataDir = __DIR__ . '/../data/snowflake';

    private Client $storageApi;

    private StagingStorageLoader $stagingStorageLoader;

    public function setUp(): void
    {
        $this->config = $this->getConfig($this->dataDir);
        $this->writer = $this->getSnowflakeWriter($this->config['parameters']);

        $tables = $this->config['parameters']['tables'];
        foreach ($tables as $table) {
            $this->writer->drop($table['dbName']);
        }

        $this->storageApi = new Client([
            'url' => getenv('KBC_URL'),
            'token' => getenv('STORAGE_API_TOKEN'),
        ]);

        $bucketId = 'in.c-test-wr-db-snowflake';
        if ($this->storageApi->bucketExists($bucketId)) {
            $this->storageApi->dropBucket($bucketId, ['force' => true]);
        }

        $this->stagingStorageLoader = new StagingStorageLoader($this->dataDir, $this->storageApi);
    }

    private function getInputCsv(string $tableId): string
    {
        return sprintf($this->dataDir . '/in/tables/%s.csv', $tableId);
    }

    private function loadDataToStagingStorage(string $tableId): array
    {
        return $this->stagingStorageLoader->upload($tableId);
    }

    public function testCreateConnection(): void
    {
        $connection = $this->writer->createSnowflakeConnection($this->config['parameters']['db']);

        $result = $connection->fetchAll('SELECT current_date;');
        $this->assertNotEmpty($result);

        try {
            $this->writer->createConnection($this->config['parameters']['db']);
            $this->fail('Create connection via Common inteface method should fail');
        } catch (ApplicationException $e) {
            $this->assertStringContainsString('Method not implemented', $e->getMessage());
        }
    }

    public function testGetConnection(): void
    {
        $connection = $this->writer->getSnowflakeConnection();

        $result = $connection->fetchAll('SELECT current_date;');
        $this->assertNotEmpty($result);

        try {
            $this->writer->getConnection();
            $this->fail('Getting connection via Common inteface method should fail');
        } catch (ApplicationException $e) {
            $this->assertStringContainsString('Method not implemented', $e->getMessage());
        }
    }

    public function testConnection(): void
    {
        $testHandler = new TestHandler();

        $logger = new Logger($this->appName);
        $logger->pushHandler($testHandler);

        $writerFactory = new SnowflakeWriterFactory($this->config['parameters']);

        /** @var Snowflake $writer */
        $writer =  $writerFactory->create($logger);
        $this->assertCount(0, $testHandler->getRecords());

        $writer->testConnection();

        $records = $testHandler->getRecords();

        $this->assertCount(1, $records);

        $this->assertStringContainsString('Executing query', $records[0]['message']);
        $this->assertEquals('INFO', $records[0]['level_name']);
    }

    public function testDrop(): void
    {
        $conn = $this->writer->getSnowflakeConnection();

        $conn->query('CREATE TABLE "dropMe" (
          id INT PRIMARY KEY,
          firstname VARCHAR(30) NOT NULL,
          lastname VARCHAR(30) NOT NULL)');

        $this->assertTrue($this->writer->tableExists('dropMe'));

        $this->writer->drop('dropMe');
        $this->assertFalse($this->writer->tableExists('dropMe'));
    }

    public function createData(): array
    {
        return [
            [true, 'TRANSIENT'],
            [false, 'TRANSIENT'],
        ];
    }

    /**
     * @dataProvider createData
     */
    public function testCreate(bool $incrementalValue, string $expectedKind): void
    {
        $tables = array_filter(
            (array) $this->config['parameters']['tables'],
            function ($table) use ($incrementalValue) {
                return $table['incremental'] === $incrementalValue;
            }
        );

        $this->assertGreaterThanOrEqual(1, count($tables));

        foreach ($tables as $table) {
            $this->writer->drop($table['dbName']);
            $this->assertFalse($this->writer->tableExists($table['dbName']));

            $this->writer->create($table);
            $this->assertTrue($this->writer->tableExists($table['dbName']));

            // check table type
            $tablesInfo = $this->writer->getSnowflakeConnection()->fetchAll(sprintf(
                "SHOW TABLES LIKE '%s';",
                $table['dbName']
            ));

            $this->assertCount(1, $tablesInfo);

            $tableInfo = reset($tablesInfo);
            $this->assertEquals($this->config['parameters']['db']['schema'], $tableInfo['schema_name']);
            $this->assertEquals($this->config['parameters']['db']['database'], $tableInfo['database_name']);
            $this->assertEquals($table['dbName'], $tableInfo['name']);
            $this->assertEquals($expectedKind, $tableInfo['kind']);
        }
    }

    public function testCreateIfNotExists(): void
    {
        $table = reset($this->config['parameters']['tables']);
        $dbName = $table['dbName'];

        $this->assertFalse($this->writer->tableExists($dbName));

        $this->writer->createIfNotExists($table);
        $this->assertTrue($this->writer->tableExists($dbName));

        $this->writer->createIfNotExists($table);
        $this->assertTrue($this->writer->tableExists($dbName));
    }

    public function testSwap(): void
    {
        $table1 = $this->config['parameters']['tables'][0];
        $table2 = $this->config['parameters']['tables'][1];

        $this->writer->create($table1);
        $this->writer->create($table2);

        $table1Columns = $this->writer->getSnowflakeConnection()->fetchAll("DESCRIBE TABLE \"{$table1['dbName']}\"");
        $table2Columns = $this->writer->getSnowflakeConnection()->fetchAll("DESCRIBE TABLE \"{$table2['dbName']}\"");
        $this->assertNotEquals($table1Columns, $table2Columns);

        $this->writer->swapTables($table1['dbName'], $table2['dbName']);

        $this->assertTrue($this->writer->tableExists($table1['dbName']));
        $this->assertTrue($this->writer->tableExists($table2['dbName']));

        $tableSwap1Columns = $this->writer->getSnowflakeConnection()->fetchAll("DESCRIBE TABLE \"{$table1['dbName']}\"");
        $tableSwap2Columns = $this->writer->getSnowflakeConnection()->fetchAll("DESCRIBE TABLE \"{$table2['dbName']}\"");

        $this->assertEquals($table1Columns, $tableSwap2Columns);
        $this->assertEquals($table2Columns, $tableSwap1Columns);
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
        $tables = array_filter(
            (array) $this->config['parameters']['tables'],
            function ($table) use ($incrementalValue) {
                return $table['incremental'] === $incrementalValue;
            }
        );

        $this->assertGreaterThanOrEqual(1, count($tables));

        foreach ($tables as $table) {
            $this->writer->drop($table['dbName']);
            $this->assertFalse($this->writer->tableExists($table['dbName']));

            $this->writer->createStaging($table);
            $this->assertTrue($this->writer->tableExists($table['dbName']));

            // check table type
            $tablesInfo = $this->writer->getSnowflakeConnection()->fetchAll(sprintf(
                "SHOW TABLES LIKE '%s';",
                $table['dbName']
            ));

            $this->assertCount(1, $tablesInfo);

            $tableInfo = reset($tablesInfo);
            $this->assertEquals($this->config['parameters']['db']['schema'], $tableInfo['schema_name']);
            $this->assertEquals($this->config['parameters']['db']['database'], $tableInfo['database_name']);
            $this->assertEquals($table['dbName'], $tableInfo['name']);
            $this->assertEquals($expectedKind, $tableInfo['kind']);
        }
    }

    public function testStageName(): void
    {
        $this->assertFalse($this->writer->generateStageName((string) getenv('KBC_RUNID')) === Snowflake::STAGE_NAME);
    }

    public function testTmpName(): void
    {
        $tableName = 'firstTable';

        $tmpName = $this->writer->generateTmpName($tableName);
        $this->assertMatchesRegularExpression('/' . $tableName . '/ui', $tmpName);
        $this->assertMatchesRegularExpression('/temp/ui', $tmpName);
        $this->assertLessThanOrEqual(256, mb_strlen($tmpName));

        $tableName = str_repeat('firstTableWithLongName', 15);

        $this->assertGreaterThanOrEqual(256, mb_strlen($tableName));
        $tmpName = $this->writer->generateTmpName($tableName);
        $this->assertMatchesRegularExpression('/temp/ui', $tmpName);
        $this->assertLessThanOrEqual(256, mb_strlen($tmpName));
    }

    public function testWriteAsync(): void
    {
        $tables = $this->config['parameters']['tables'];

        // simple table
        $table = $tables[0];

        $writer = $this->getSnowflakeWriter($this->config['parameters'], $this->getAdapter($table['tableId']));
        $writer->drop($table['dbName']);
        $writer->create($table);
        $writer->writeFromAdapter($table);

        /** @var Connection $conn */
        $conn = new Connection($this->config['parameters']['db']);

        // check if writer stage does not exists
        $stageName = $writer->generateStageName((string) getenv('KBC_RUNID'));

        $writerStages = array_filter(
            $conn->fetchAll(sprintf("SHOW STAGES LIKE '{$stageName}'")),
            function ($row) {
                return $row['owner'] === getenv('SNOWFLAKE_DB_USER');
            }
        );

        $this->assertCount(0, $writerStages);

        // validate structure and data
        $columnsInDb = $conn->fetchAll("DESCRIBE TABLE \"{$table['dbName']}\"");
        $getColumnInDb = function ($columnName) use ($columnsInDb) {
            $found = array_filter($columnsInDb, function ($currentColumn) use ($columnName) {
                return $currentColumn['name'] === $columnName;
            });
            if (empty($found)) {
                throw new \Exception("Column $columnName not found");
            }
            return reset($found);
        };

        foreach ($table['items'] as $columnConfiguration) {
            $columnInDb = $getColumnInDb($columnConfiguration['dbName']);
            if (!empty($columnConfiguration['nullable'])) {
                $this->assertEquals('Y', $columnInDb['null?']);
            } else {
                $this->assertEquals('N', $columnInDb['null?']);
            }
        }

        $res = $conn->fetchAll(sprintf('SELECT * FROM "%s" ORDER BY "id" ASC', $table['dbName']));

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(['id','name','glasses', 'age']);
        foreach ($res as $row) {
            $csv->writeRow($row);

            // null test - age column is nullable
            if (!is_numeric($row['age'])) {
                $this->assertNull($row['age']);
            }
            $this->assertNotNull($row['name']);
        }

        $this->assertFileEquals($this->getInputCsv($table['tableId']), $csv->getPathname());
    }

    public function testUpsert(): void
    {
        $tables = $this->config['parameters']['tables'];
        foreach ($tables as $table) {
            $this->writer->drop($table['dbName']);
        }
        $table = $tables[0];

        $targetTable = $table;
        $table['dbName'] .= $table['incremental']?'_temp_' . uniqid():'';

        // first write
        $writer = $this->getSnowflakeWriter($this->config['parameters'], $this->getAdapter($table['tableId']));
        $writer->create($targetTable);
        $writer->writeFromAdapter($targetTable);

        // second write
        $writer = $this->getSnowflakeWriter(
            $this->config['parameters'],
            $this->getAdapter($table['tableId'] . '_increment')
        );
        $writer->create($table);
        $writer->writeFromAdapter($table);

        $writer->upsert($table, $targetTable['dbName']);

        /** @var Connection $conn */
        $conn = new Connection($this->config['parameters']['db']);
        $res = $conn->fetchAll("SELECT * FROM \"{$targetTable['dbName']}\" ORDER BY \"id\" ASC");

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(['id', 'name', 'glasses', 'age']);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $expectedFilename = $this->getInputCsv($table['tableId'] . '_merged');

        $this->assertFileEquals($expectedFilename, $csv->getPathname());
    }

    public function testDefaultWarehouse(): void
    {
        $config = $this->config;

        $warehouse = $config['parameters']['db']['warehouse'];

        $this->setUserDefaultWarehouse(null);
        $this->assertEmpty($this->writer->getUserDefaultWarehouse());

        // run without warehouse param
        unset($config['parameters']['db']['warehouse']);

        try {
            $this->getSnowflakeWriter($config['parameters']);
            $this->fail('Create writer without warehouse should fail');
        } catch (UserException $e) {
            $this->assertMatchesRegularExpression('/Snowflake user has any \"DEFAULT_WAREHOUSE\" specified/ui', $e->getMessage());
        }

        // run with warehouse param
        $config = $this->config;
        $tables = $config['parameters']['tables'];
        $table = $tables[0];

        $writer = $this->getSnowflakeWriter($this->config['parameters'], $this->getAdapter($table['tableId']));
        $writer->create($table);
        $writer->writeFromAdapter($table);

        // restore default warehouse
        $this->setUserDefaultWarehouse($warehouse);
        $this->assertEquals($warehouse, $this->writer->getUserDefaultWarehouse());
    }

    public function testInvalidWarehouse(): void
    {
        $parameters = $this->config['parameters'];
        $parameters['db']['warehouse'] = uniqid();

        try {
            $this->getSnowflakeWriter($parameters);
            $this->fail('Creating writer should fail with UserError');
        } catch (UserException $e) {
            $this->assertStringContainsString('Invalid warehouse', $e->getMessage());
        }
    }

    public function testInvalidSchema(): void
    {
        $parameters = $this->config['parameters'];
        $parameters['db']['schema'] = uniqid();
        try {
            $this->getSnowflakeWriter($parameters);
            $this->fail('Creating writer should fail with UserError');
        } catch (UserException $e) {
            $this->assertStringContainsString('Invalid schema', $e->getMessage());
        }
    }

    public function testCheckPrimaryKey(): void
    {
        $table = $this->config['parameters']['tables'][0];
        $table['primaryKey'] = ['id', 'name'];

        $this->writer->create($table);

        // test with keys in different order
        $this->writer->checkPrimaryKey(['name', 'id'], $table['dbName']);

        // no exception thrown, that's good
        $this->assertTrue(true);
    }

    public function testCheckPrimaryKeyError(): void
    {
        $table = $this->config['parameters']['tables'][0];

        $tableConfigWithOtherPrimaryKeys = $table;
        $tableConfigWithOtherPrimaryKeys['items'][0]['dbName'] = 'code';
        $tableConfigWithOtherPrimaryKeys['primaryKey'] = ['code'];

        $this->writer->create($tableConfigWithOtherPrimaryKeys);

        try {
            $this->writer->checkPrimaryKey($table['primaryKey'], $table['dbName']);
            $this->fail('Primary key check should fail');
        } catch (UserException $e) {
            $this->assertStringContainsString('Primary key(s) in configuration does NOT match with keys in DB table.', $e->getMessage());
        }
    }

    public function testUpsertCheckPrimaryKeyError(): void
    {
        $table = $this->config['parameters']['tables'][0];
        $table['primaryKey'] = ['id'];

        $tmpTable = $table;
        $tmpTable['dbName'] = $this->writer->generateTmpName($table['dbName']);

        $this->writer->create($table);
        $this->writer->create($tmpTable);

        try {
            $table['primaryKey'] = ['id', 'name'];
            $this->writer->upsert($table, $tmpTable['dbName']);
            $this->fail('Primary key check should fail');
        } catch (UserException $e) {
            $this->assertStringContainsString('Primary key(s) in configuration does NOT match with keys in DB table.', $e->getMessage());
        }
    }

    public function testUpsertAddMissingPrimaryKey(): void
    {
        $table = $this->config['parameters']['tables'][0];
        $table['primaryKey'] = [];

        $tmpTable = $table;
        $tmpTable['dbName'] = $this->writer->generateTmpName($table['dbName']);

        $this->writer->create($table);
        $this->writer->create($tmpTable);

        $this->writer->checkPrimaryKey([], $tmpTable['dbName']);

        $table['primaryKey'] = ['id', 'name'];
        $this->writer->upsert($table, $tmpTable['dbName']);

        $this->writer->checkPrimaryKey(['id', 'name'], $tmpTable['dbName']);

        $this->expectNotToPerformAssertions();
    }

    /**
     * @dataProvider queryTaggingProvider
     */
    public function testQueryTagging(array $additionalDbConfig, string $expectedRunId): void
    {
        $dbConfig = array_merge($this->config['parameters']['db'], $additionalDbConfig);
        $connection = $this->writer->createSnowflakeConnection($dbConfig);

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
            '
        );

        $runId = sprintf('{"runId":"%s"}', $expectedRunId);

        $this->assertEquals($runId, $queries[0]['QUERY_TAG']);
    }

    private function setUserDefaultWarehouse(?string $warehouse = null): void
    {
        $user = $this->writer->getCurrentUser();
        $conn = $this->writer->getSnowflakeConnection();

        if ($warehouse) {
            $sql = sprintf(
                'ALTER USER %s SET DEFAULT_WAREHOUSE = %s;',
                $conn->quoteIdentifier($user),
                $conn->quoteIdentifier($warehouse)
            );
            $conn->query($sql);

            $this->assertEquals($warehouse, $this->writer->getUserDefaultWarehouse());
        } else {
            $sql = sprintf(
                'ALTER USER %s SET DEFAULT_WAREHOUSE = null;',
                $conn->quoteIdentifier($user)
            );
            $conn->query($sql);

            $this->assertEmpty($this->writer->getUserDefaultWarehouse());
        }
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

    private function getAdapter(string $table): IAdapter
    {
        $loadFile = $this->loadDataToStagingStorage($table);
        if ($loadFile['stagingStorage'] === StagingStorageLoader::STORAGE_S3) {
            return new S3Adapter($loadFile['manifest']);
        } elseif ($loadFile['stagingStorage'] === StagingStorageLoader::STORAGE_ABS) {
            return new AbsAdapter($loadFile['manifest']);
        }

        throw new ApplicationException(sprintf('Staging storage type "%s" not recognized', $loadFile['stage']));
    }
}
