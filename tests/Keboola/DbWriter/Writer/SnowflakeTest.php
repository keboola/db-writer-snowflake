<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 05/11/15
 * Time: 13:33
 */

namespace Keboola\DbWriter\Writer;

use Keboola\Csv\CsvFile;
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
        $this->config['parameters']['db']['schema'] = 'development';
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
        $conn = $this->writer->getConnection();
        $res = $conn->fetchAll(sprintf('SELECT * FROM "%s" ORDER BY id ASC', $table['dbName']));

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(["id","name","glasses"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $this->assertFileEquals($this->getInputCsv($table['tableId']), $resFilename);
    }

    public function testUpsert()
    {
        /** @var Connection $conn */
        $conn = $this->writer->getConnection();
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

        $res = $conn->fetchAll("SELECT * FROM \"{$targetTable['dbName']}\" ORDER BY id ASC");

        $resFilename = tempnam('/tmp', 'db-wr-test-tmp');
        $csv = new CsvFile($resFilename);
        $csv->writeRow(["id", "name", "glasses"]);
        foreach ($res as $row) {
            $csv->writeRow($row);
        }

        $expectedFilename = $this->getInputCsv($table['tableId'] . "_merged");

        $this->assertFileEquals($expectedFilename, $resFilename);
    }
}
