<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Snowflake\Connection;
use Keboola\DbWriter\Snowflake\DataType\Definition;
use Keboola\DbWriter\Snowflake\Test\S3Loader;
use Keboola\DbWriter\Writer\Snowflake;
use Keboola\DbWriter\WriterFactory;
use Keboola\StorageApi\Client;
use Monolog\Handler\TestHandler;

class SnowflakeTest extends BaseTest
{
    protected $dataDir = __DIR__ . '/../data/snowflake';

    /** @var Snowflake */
    private $writer;

    private $config;

    /** @var Client */
    private $storageApi;

    /** @var S3Loader */
    private $s3Loader;

    public function setUp(): void
    {
        $this->config = $this->getConfig($this->dataDir);

        $writer = $this->getWriter($this->config['parameters']);
        if ($writer instanceof Snowflake) {
            $this->writer = $writer;
        } else {
            $this->fail('Writer factory must init Snowflake Writer');
        }

        $tables = $this->config['parameters']['tables'];
        foreach ($tables as $table) {
            $this->writer->drop($table['dbName']);
        }

        $this->storageApi = new Client([
            'token' => getenv('STORAGE_API_TOKEN'),
        ]);

        $bucketId = 'in.c-test-wr-db-snowflake';
        if ($this->storageApi->bucketExists($bucketId)) {
            $this->storageApi->dropBucket($bucketId, ['force' => true]);
        }

        $this->s3Loader = new S3Loader($this->dataDir, $this->storageApi);
    }

    private function getInputCsv($tableId): string
    {
        return sprintf($this->dataDir . "/in/tables/%s.csv", $tableId);
    }

    private function loadDataToS3($tableId): array
    {
        return $this->s3Loader->upload($tableId);
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
            $this->assertContains('Method not implemented', $e->getMessage());
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
            $this->assertContains('Method not implemented', $e->getMessage());
        }
    }

    public function testConnection(): void
    {
        $testHandler = new TestHandler();

        $logger = new Logger($this->appName);
        $logger->pushHandler($testHandler);

        $writerFactory = new WriterFactory($this->config['parameters']);

        /** @var Snowflake $writer */
        $writer =  $writerFactory->create($logger);

        if (!$writer instanceof Snowflake) {
            $this->fail("Writer factory must init Snowflake Writer");
        }

        $this->assertCount(0, $testHandler->getRecords());

        $writer->testConnection();

        $records = $testHandler->getRecords();

        $this->assertCount(1, $records);

        $this->assertContains('Executing query', $records[0]['message']);
        $this->assertEquals('INFO', $records[0]['level_name']);
    }

    public function testDrop(): void
    {
        $conn = $this->writer->getSnowflakeConnection();

        $conn->query("CREATE TABLE \"dropMe\" (
          id INT PRIMARY KEY,
          firstname VARCHAR(30) NOT NULL,
          lastname VARCHAR(30) NOT NULL)");

        $this->assertTrue($this->writer->tableExists('dropMe'));

        $this->writer->drop('dropMe');
        $this->assertFalse($this->writer->tableExists('dropMe'));
    }

    public function testGetTimestampTypeMapping(): void
    {
        $timestampTypeMapping = $this->writer->getTimestampTypeMapping();

        $this->assertTrue(in_array($timestampTypeMapping, [
            Definition::TIMESTAMP_TYPE_MAPPING_LTZ,
            Definition::TIMESTAMP_TYPE_MAPPING_NTZ,
        ]));
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
            $this->config['parameters']['tables'],
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
            $this->config['parameters']['tables'],
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

    public function testCreateTextColumnWithDefaultValue(): void
    {
        $connection = $this->writer->getSnowflakeConnection();

        $tableName = 'textColumnWithDefault';
        $defaultValue = 'test default';

        $this->writer->drop($tableName);

        $this->writer->create([
            'dbName' => $tableName,
            'items' => [
                [
                    'name' => 'id',
                    'dbName' => 'id',
                    'type' => 'INT',
                ],
                [
                    'name' => 'description',
                    'dbName' => 'descriptionWithDefault',
                    'type' => 'TEXT',
                    'default' => $defaultValue,
                ],
            ],
        ]);

        $sql = 'INSERT INTO %s (%s) VALUES (%s);';

        $connection->query(sprintf(
            $sql,
            $connection->quoteIdentifier($tableName),
            $connection->quoteIdentifier('id'),
            1
        ));


        $rows = $connection->fetchAll(sprintf('SELECT * FROM %s;', $connection->quoteIdentifier($tableName)));
        $this->assertCount(1, $rows);

        $row = reset($rows);

        $this->assertNotEmpty($row['descriptionWithDefault']);
        $this->assertEquals($defaultValue, $row['descriptionWithDefault']);
    }

    public function testStageName(): void
    {
        $this->assertFalse($this->writer->generateStageName((string) getenv('KBC_RUNID')) === Snowflake::STAGE_NAME);
    }

    public function testTmpName(): void
    {
        $tableName = 'firstTable';

        $tmpName = $this->writer->generateTmpName($tableName);
        $this->assertRegExp('/' . $tableName . '/ui', $tmpName);
        $this->assertRegExp('/temp/ui', $tmpName);
        $this->assertLessThanOrEqual(256, mb_strlen($tmpName));

        $tableName = str_repeat('firstTableWithLongName', 15);

        $this->assertGreaterThanOrEqual(256, mb_strlen($tableName));
        $tmpName = $this->writer->generateTmpName($tableName);
        $this->assertRegExp('/temp/ui', $tmpName);
        $this->assertLessThanOrEqual(256, mb_strlen($tmpName));
    }

    public function testWriteAsync(): void
    {
        $tables = $this->config['parameters']['tables'];

        // simple table
        $table = $tables[0];
        $s3manifest = $this->loadDataToS3($table['tableId']);

        $this->writer->drop($table['dbName']);
        $this->writer->create($table);
        $this->writer->writeFromS3($s3manifest, $table);

        /** @var Connection $conn */
        $conn = new Connection($this->config['parameters']['db']);

        // check if writer stage does not exists
        $stageName = $this->writer->generateStageName((string) getenv('KBC_RUNID'));

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
        $csv->writeRow(["id","name","glasses", "age"]);
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

        $s3Manifest = $this->loadDataToS3($table['tableId']);

        $targetTable = $table;
        $table['dbName'] .= $table['incremental']?'_temp_' . uniqid():'';

        // first write
        $this->writer->create($targetTable);
        $this->writer->writeFromS3($s3Manifest, $targetTable);

        // second write
        $s3Manifest = $this->loadDataToS3($table['tableId'] . "_increment");
        $this->writer->create($table);
        $this->writer->writeFromS3($s3Manifest, $table);

        $this->writer->upsert($table, $targetTable['dbName']);

        /** @var Connection $conn */
        $conn = new Connection($this->config['parameters']['db']);
        $res = $conn->fetchAll("SELECT * FROM \"{$targetTable['dbName']}\" ORDER BY \"id\" ASC");

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(["id", "name", "glasses", "age"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $expectedFilename = $this->getInputCsv($table['tableId'] . "_merged");

        $this->assertFileEquals($expectedFilename, $csv->getPathname());
    }

    public function testTruncate(): void
    {
        $tables = $this->config['parameters']['tables'];

        // simple table
        $table = $tables[0];
        $s3manifest = $this->loadDataToS3($table['tableId']);

        $this->writer->drop($table['dbName']);
        $this->writer->create($table);
        $this->writer->writeFromS3($s3manifest, $table);

        /** @var Connection $conn */
        $conn = new Connection($this->config['parameters']['db']);

        $res = $conn->fetchAll(sprintf('SELECT * FROM "%s" ORDER BY "id" ASC', $table['dbName']));
        $this->assertGreaterThanOrEqual(1, count($res));

        $this->writer->truncate($table['dbName']);
        $this->writer->tableExists($table['dbName']);

        $res = $conn->fetchAll(sprintf('SELECT * FROM "%s" ORDER BY "id" ASC', $table['dbName']));
        $this->assertCount(0, $res);
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
            $this->getWriter($config['parameters']);
            $this->fail('Create writer without warehouse should fail');
        } catch (UserException $e) {
            $this->assertRegExp('/Snowflake user has any \"DEFAULT_WAREHOUSE\" specified/ui', $e->getMessage());
        }

        // run with warehouse param
        $config = $this->config;

        /** @var Snowflake $writer */
        $writer = $this->getWriter($config['parameters']);

        $tables = $config['parameters']['tables'];
        $table = $tables[0];

        $s3Manifest = $this->loadDataToS3($table['tableId']);

        $writer->create($table);
        $writer->writeFromS3($s3Manifest, $table);

        // restore default warehouse
        $this->setUserDefaultWarehouse($warehouse);
        $this->assertEquals($warehouse, $this->writer->getUserDefaultWarehouse());
    }

    public function testInvalidWarehouse(): void
    {
        $parameters = $this->config['parameters'];
        $parameters['db']['warehouse'] = uniqid();

        try {
            $this->getWriter($parameters);
            $this->fail('Creating writer should fail with UserError');
        } catch (UserException $e) {
            $this->assertContains('Invalid warehouse', $e->getMessage());
        }
    }

    public function testInvalidSchema(): void
    {
        $parameters = $this->config['parameters'];
        $parameters['db']['schema'] = uniqid();
        try {
            $this->getWriter($parameters);
            $this->fail('Creating writer should fail with UserError');
        } catch (UserException $e) {
            $this->assertContains('Invalid schema', $e->getMessage());
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
            $this->assertContains('Primary key(s) in configuration does NOT match with keys in DB table.', $e->getMessage());
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
            $this->assertContains('Primary key(s) in configuration does NOT match with keys in DB table.', $e->getMessage());
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
    }

    public function testTableInfo(): void
    {
        $table = reset($this->config['parameters']['tables']);

        $columns = array_filter($table['items'], function ($item) {
            return (strtolower($item['type']) !== 'ignore');
        });

        $this->writer->drop($table['dbName']);
        $this->writer->create($table);

        $columnsInfo = $this->writer->getTableInfo($table['dbName']);

        $this->assertEquals(count($columns), count($columnsInfo));

        $glassesColumnInfo = null;
        foreach ($columnsInfo as $columnInfo) {
            $this->assertArrayHasKey('name', $columnInfo);
            $this->assertArrayHasKey('kind', $columnInfo);
            $this->assertArrayHasKey('type', $columnInfo);
            $this->assertArrayHasKey('null?', $columnInfo);
            $this->assertArrayHasKey('default', $columnInfo);

            if ($columnInfo['name'] === 'glasses') {
                $glassesColumnInfo = $columnInfo;
            }
        }

        $this->assertTrue(is_array($glassesColumnInfo));
        $this->assertEquals('glasses', $glassesColumnInfo['name']);
        $this->assertEquals('COLUMN', $glassesColumnInfo['kind']);
        $this->assertEquals('VARCHAR(255)', $glassesColumnInfo['type']);
        $this->assertEquals('N', $glassesColumnInfo['null?']);
        $this->assertNull($glassesColumnInfo['default']);
    }

    public function checkColumnsData(): array
    {
        return [
            [
                [
                    [
                        'name' => 'id',
                        'dbName' => 'id',
                        'type' => 'INT',
                    ],
                    [
                        'name' => 'name',
                        'dbName' => 'name',
                        'type' => 'VARCHAR',
                    ],
                ],
            ],
            [
                [
                    [
                        'name' => 'id',
                        'dbName' => 'idRenamed',
                        'type' => 'INT',
                    ],
                    [
                        'name' => 'name',
                        'dbName' => 'nameRenamed',
                        'type' => 'VARCHAR',
                    ],
                ],
            ],
        ];
    }

    /**
     * @dataProvider checkColumnsData
     */
    public function testCheckColumns(array $dbColumns): void
    {
        $table = [
            'tableId' => 'testCheckColumns',
            'dbName' => 'testCheckColumns',
            'export' => true,
            'incremental' => true,
            'primaryKey' => [],
            'items' => $dbColumns,
        ];

        $this->writer->drop($table['dbName']);
        $this->writer->create($table);

        // test with columns in different order
        $this->writer->checkColumns(array_reverse($table['items']), $table['dbName']);

        // no exception thrown, that's good
        $this->assertTrue(true);
    }

    public function checkColumnsErrorData(): array
    {
        return [
            [
                [
                    [
                        'name' => 'id',
                        'dbName' => 'id',
                        'type' => 'INT',
                    ],
                    [
                        'name' => 'name',
                        'dbName' => 'name',
                        'type' => 'VARCHAR',
                    ],
                ],
                [
                    [
                        'name' => 'id',
                        'dbName' => 'id',
                        'type' => 'INT',
                    ],
                ],
                'Some columns are missing in the mapping',
                'name',
            ],
            [
                [
                    [
                        'name' => 'id',
                        'dbName' => 'idRenamed',
                        'type' => 'INT',
                    ],
                    [
                        'name' => 'name',
                        'dbName' => 'nameRenamed',
                        'type' => 'VARCHAR',
                    ],
                ],
                [
                    [
                        'name' => 'id',
                        'dbName' => 'idRenamed',
                        'type' => 'INT',
                    ],
                ],
                'Some columns are missing in the mapping',
                'nameRenamed',
            ],
            [
                [
                    [
                        'name' => 'name',
                        'dbName' => 'name',
                        'type' => 'VARCHAR',
                    ],
                ],
                [
                    [
                        'name' => 'id',
                        'dbName' => 'id',
                        'type' => 'INT',
                    ],
                    [
                        'name' => 'name',
                        'dbName' => 'name',
                        'type' => 'VARCHAR',
                    ],
                ],
                'Some columns are missing in DB table',
                'id',
            ],
            [
                [
                    [
                        'name' => 'name',
                        'dbName' => 'nameRenamed',
                        'type' => 'VARCHAR',
                    ],
                ],
                [
                    [
                        'name' => 'id',
                        'dbName' => 'idRenamed',
                        'type' => 'INT',
                    ],
                    [
                        'name' => 'name',
                        'dbName' => 'nameRenamed',
                        'type' => 'VARCHAR',
                    ],
                ],
                'Some columns are missing in DB table',
                'idRenamed',
            ],
        ];
    }

    /**
     * @dataProvider checkColumnsErrorData
     */
    public function testCheckColumnsError(array $dbColumns, array $mappingColumns, string $expError, string $expErrorColumn): void
    {
        $table = [
            'tableId' => 'testCheckColumnsError',
            'dbName' => 'testCheckColumnsError',
            'export' => true,
            'incremental' => true,
            'primaryKey' => [],
            'items' => $dbColumns,
        ];

        $this->writer->drop($table['dbName']);
        $this->writer->create($table);

        // test with columns in different order
        try {
            $this->writer->checkColumns($mappingColumns, $table['dbName']);
            $this->fail('Check columns with different mapping should produce error');
        } catch (UserException $e) {
            $this->assertContains($expError, $e->getMessage());
            $this->assertContains($expErrorColumn, $e->getMessage());
        }
    }

    public function checkDataTypesData(): array
    {
        return [
            [
                [
                    [
                        'name' => 'name',
                        'dbName' => 'nameDefaultLength',
                        'type' => 'VARCHAR',
                    ],
                    [
                        'name' => 'age',
                        'dbName' => 'ageDatatypeAlias',
                        'type' => 'BIGINT',
                    ],
                    [
                        'name' => 'glasses',
                        'dbName' => 'glassesNotNullable',
                        'type' => 'VARCHAR',
                    ],
                    [
                        'name' => 'country',
                        'dbName' => 'countryStringWithDefault',
                        'type' => 'VARCHAR',
                        'default' => 'cz',
                    ],
                    [
                        'name' => 'region',
                        'dbName' => 'regionIntegerWithDefault',
                        'type' => 'BIGINT',
                        'default' => '1',
                    ],
                    [
                        'name' => 'position',
                        'dbName' => 'positionEmptyStringDefault',
                        'type' => 'VARCHAR',
                        'default' => '',
                    ],
                ],
                [
                    [
                        'name' => 'name',
                        'dbName' => 'nameDefaultLength',
                        'type' => 'VARCHAR',
                        'size' => 16777216,
                    ],
                    [
                        'name' => 'age',
                        'dbName' => 'ageDatatypeAlias',
                        'type' => 'SMALLINT',
                    ],
                    [
                        'name' => 'glasses',
                        'dbName' => 'glassesNotNullable',
                        'type' => 'VARCHAR',
                        'nullable' => false,
                    ],
                    [
                        'name' => 'country',
                        'dbName' => 'countryStringWithDefault',
                        'type' => 'VARCHAR',
                        'default' => 'cz',
                    ],
                    [
                        'name' => 'region',
                        'dbName' => 'regionIntegerWithDefault',
                        'type' => 'BIGINT',
                        'default' => 1,
                    ],
                    [
                        'name' => 'position',
                        'dbName' => 'positionEmptyStringDefault',
                        'type' => 'VARCHAR',
                        'default' => '',
                    ],
                ],
            ],
        ];
    }

    /**
     * @dataProvider checkDataTypesData
     */
    public function testCheckDataTypes(array $dbColumns, array $mappingColumns): void
    {
        $table = [
            'tableId' => 'testCheckDataTypes',
            'dbName' => 'testCheckDataTypes',
            'export' => true,
            'incremental' => true,
            'primaryKey' => [],
            'items' => $dbColumns,
        ];

        $this->writer->drop($table['dbName']);
        $this->writer->create($table);

        // test with columns in different order
        $this->writer->checkDataTypes(array_reverse($mappingColumns), $table['dbName']);

        // no exception thrown, that's good
        $this->assertTrue(true);
    }

    public function checkDataTypesErrorData(): array
    {
        return [
            [
                [
                    [
                        'name' => 'name',
                        'dbName' => 'name',
                        'type' => 'VARCHAR',
                    ],
                ],
                [
                    [
                        'name' => 'name',
                        'dbName' => 'name',
                        'type' => 'VARCHAR',
                        'size' => 255,
                    ],
                ],
            ],
            [
                [
                    [
                        'name' => 'name',
                        'dbName' => 'name',
                        'type' => 'VARCHAR',
                        'size' => 50,
                    ],
                ],
                [
                    [
                        'name' => 'name',
                        'dbName' => 'name',
                        'type' => 'VARCHAR',
                        'size' => 255,
                    ],
                ],
            ],
            [
                [
                    [
                        'name' => 'age',
                        'dbName' => 'age',
                        'type' => 'BIGINT',
                    ],
                ],
                [
                    [
                        'name' => 'age',
                        'dbName' => 'age',
                        'type' => 'FLOAT',
                    ],
                ],
            ],
            [
                [
                    [
                        'name' => 'glasses',
                        'dbName' => 'glasses',
                        'type' => 'VARCHAR',
                    ],
                ],
                [
                    [
                        'name' => 'glasses',
                        'dbName' => 'glasses',
                        'type' => 'VARCHAR',
                        'nullable' => true,
                    ],
                ],
            ],
            [
                [
                    [
                        'name' => 'country',
                        'dbName' => 'country',
                        'type' => 'VARCHAR',
                        'default' => 'cz',
                    ],
                ],
                [
                    [
                        'name' => 'country',
                        'dbName' => 'country',
                        'type' => 'VARCHAR',
                        'default' => 'en',
                    ],
                ],
            ],
            [
                [
                    [
                        'name' => 'country',
                        'dbName' => 'country',
                        'type' => 'VARCHAR',
                        'nullable' => true,
                        'default' => '',
                    ],
                ],
                [
                    [
                        'name' => 'country',
                        'dbName' => 'country',
                        'type' => 'VARCHAR',
                        'nullable' => true,
                        'default' => null,
                    ],
                ],
            ],
        ];
    }

    /**
     * @dataProvider checkDataTypesErrorData
     */
    public function testCheckDataTypesError(array $dbColumns, array $mappingColumns): void
    {
        $table = [
            'tableId' => 'testCheckDataTypes',
            'dbName' => 'testCheckDataTypes',
            'export' => true,
            'incremental' => true,
            'primaryKey' => [],
            'items' => $dbColumns,
        ];

        $this->writer->drop($table['dbName']);
        $this->writer->create($table);

        try {
            $this->writer->checkDataTypes($mappingColumns, $table['dbName']);
            $this->fail('Check columns with different datatype mapping should produce error');
        } catch (UserException $e) {
            $this->assertContains('Different mapping between incremental load and workspace for columns', $e->getMessage());
        }
    }

    private function setUserDefaultWarehouse(?string $warehouse = null): void
    {
        $user = $this->writer->getCurrentUser();
        $conn = $this->writer->getSnowflakeConnection();

        if ($warehouse) {
            $sql = sprintf(
                "ALTER USER %s SET DEFAULT_WAREHOUSE = %s;",
                $conn->quoteIdentifier($user),
                $conn->quoteIdentifier($warehouse)
            );
            $conn->query($sql);

            $this->assertEquals($warehouse, $this->writer->getUserDefaultWarehouse());
        } else {
            $sql = sprintf(
                "ALTER USER %s SET DEFAULT_WAREHOUSE = null;",
                $conn->quoteIdentifier($user)
            );
            $conn->query($sql);

            $this->assertEmpty($this->writer->getUserDefaultWarehouse());
        }
    }
}
