<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 05/11/15
 * Time: 13:33
 */

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Snowflake\Connection;
use Keboola\DbWriter\Snowflake\Test\S3Loader;
use Keboola\DbWriter\Test\BaseTest;
use Keboola\StorageApi\Client;

class SnowflakeTest extends BaseTest
{
    const DRIVER = 'snowflake';

    /** @var Snowflake */
    private $writer;

    private $config;

    /** @var Client */
    private $storageApi;

    /** @var S3Loader */
    private $s3Loader;

    public function setUp()
    {
        $this->config = $this->getConfig(self::DRIVER);
        $this->config['parameters']['writer_class'] = 'Snowflake';
        $this->config['parameters']['db']['schema'] = $this->getEnv(self::DRIVER, 'DB_SCHEMA');
        $this->config['parameters']['db']['warehouse'] = $this->getEnv(self::DRIVER, 'DB_WAREHOUSE');
        $this->config['parameters']['db']['password'] = $this->config['parameters']['db']['#password'];

        $this->writer = $this->getWriter($this->config['parameters']);

        $tables = $this->config['parameters']['tables'];
        foreach ($tables as $table) {
            $this->writer->drop($table['dbName']);
        }

        $this->storageApi = new Client([
            'token' => getenv('STORAGE_API_TOKEN')
        ]);

        $bucketId = 'in.c-test-wr-db-snowflake';
        if ($this->storageApi->bucketExists($bucketId)) {
            $this->storageApi->dropBucket($bucketId, ['force' => true]);
        }

        $this->s3Loader = new S3Loader($this->dataDir, $this->storageApi);
    }

    private function getInputCsv($tableId)
    {
        return sprintf($this->dataDir . "/in/tables/%s.csv", $tableId);
    }

    private function loadDataToS3($tableId)
    {
        return $this->s3Loader->upload($tableId);
    }

    public function testDrop()
    {
        /** @var Connection $conn */
        $conn = $this->writer->getConnection();
        $conn->query("CREATE TABLE \"dropMe\" (
          id INT PRIMARY KEY,
          firstname VARCHAR(30) NOT NULL,
          lastname VARCHAR(30) NOT NULL)");

        $this->writer->drop("dropMe");

        $res = $conn->fetchAll("
            SELECT *
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_name = 'dropMe'
        ");

        $this->assertEmpty($res);
    }

    public function testCreate()
    {
        $tables = $this->config['parameters']['tables'];

        foreach ($tables as $table) {
            $this->writer->drop($table['dbName']);
            $this->writer->create($table);
        }

        /** @var Connection $conn */
        $conn = $this->writer->getConnection();
        $res = $conn->fetchAll("
            SELECT *
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_name = '{$tables[0]['dbName']}'
        ");

        $this->assertEquals('simple', $res[0]['TABLE_NAME']);
    }

    public function testStageName()
    {
        $this->assertFalse($this->writer->generateStageName(getenv('KBC_RUNID')) === Snowflake::STAGE_NAME);
    }

    public function testWriteAsync()
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
        $stageName = $this->writer->generateStageName(getenv('KBC_RUNID'));

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

        $this->assertFileEquals($this->getInputCsv($table['tableId']), $resFilename);
    }

    public function testUpsert()
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

        $this->assertFileEquals($expectedFilename, $resFilename);
    }

    public function testDefaultWarehouse()
    {
        $config = $this->config;

        /** @var Connection $conn */
        $conn = $this->writer->getConnection();

        $user = $config['parameters']['db']['user'];
        $warehouse = $config['parameters']['db']['warehouse'];

        // reset warehouse
        $sql = sprintf(
            "ALTER USER %s SET DEFAULT_WAREHOUSE = null;",
            $conn->quoteIdentifier($user)
        );
        $conn->query($sql);

        $this->assertEmpty($this->writer->getUserDefaultWarehouse());

        // run without warehouse param
        unset($config['parameters']['db']['warehouse']);

        /** @var Snowflake $writer */
        $writer = $this->getWriter($config['parameters']);

        $tables = $config['parameters']['tables'];
        foreach ($tables as $table) {
            $writer->drop($table['dbName']);
        }
        $table = $tables[0];

        $s3Manifest = $this->loadDataToS3($table['tableId']);

        try {
            $writer->create($table);
            $writer->writeFromS3($s3Manifest, $table);
            $this->fail('Run writer without warehouse should fail');
        } catch (UserException $e) {
            $this->assertRegExp('/No active warehouse/ui', $e->getMessage());
        }

        // run with warehouse param
        $config = $this->config;

        /** @var Snowflake $writer */
        $writer = $this->getWriter($config['parameters']);

        $writer->create($table);
        $writer->writeFromS3($s3Manifest, $table);

        // restore default warehouse
        $sql = sprintf(
            "ALTER USER %s SET DEFAULT_WAREHOUSE = %s;",
            $conn->quoteIdentifier($user),
            $conn->quoteIdentifier($warehouse)
        );
        $conn->query($sql);
    }

    public function testCredentialsDefaultWarehouse()
    {
        $config = $this->config;
        $config['action'] = 'testConnection';
        unset($config['parameters']['tables']);

        $warehouse = $config['parameters']['db']['warehouse'];

        // empty default warehouse, specified in config
        $this->setUserDefaultWarehouse(null);

        /** @var Snowflake $writer */
        $writer = $this->getWriter($config['parameters']);
        $writer->testConnection();

        // empty default warehouse and not specified in config
        unset($config['parameters']['db']['warehouse']);

        /** @var Snowflake $writer */
        $writer = $this->getWriter($config['parameters']);

        try {
            $writer->testConnection();
            $this->fail('Test connection without warehouse and default warehouse should fail');
        } catch (UserException $e) {
            $this->assertRegExp('/Specify \"warehouse\" parameter/ui', $e->getMessage());
        }

        // bad warehouse
        $config['parameters']['db']['warehouse'] = uniqid('test');

        /** @var Snowflake $writer */
        $writer = $this->getWriter($config['parameters']);

        try {
            $writer->testConnection();
            $this->fail('Test connection with invalid warehouse ID should fail');
        } catch (UserException $e) {
            $this->assertRegExp('/Invalid warehouse/ui', $e->getMessage());
        }

        $this->setUserDefaultWarehouse($warehouse);
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

    private function setUserDefaultWarehouse($warehouse = null)
    {
        /** @var Connection $conn */
        $conn = $this->writer->getConnection();

        $user = $this->writer->getCurrentUser();

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
