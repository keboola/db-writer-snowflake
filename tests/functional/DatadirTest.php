<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake\FunctionalTests;

use Keboola\Csv\CsvFile;
use Keboola\Csv\Exception;
use Keboola\Csv\InvalidArgumentException;
use Keboola\DatadirTests\AbstractDatadirTestCase;
use Keboola\DatadirTests\DatadirTestSpecificationInterface;
use Keboola\DbWriter\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbWriter\Snowflake\TraitTests\CloseSshTunnelsTrait;
use Keboola\DbWriter\Writer\SnowflakeConnection;
use Keboola\DbWriter\Writer\SnowflakeConnectionFactory;
use Psr\Log\Test\TestLogger;
use RuntimeException;
use Symfony\Component\Filesystem\Filesystem;
use Symfony\Component\Finder\Finder;

class DatadirTest extends AbstractDatadirTestCase
{
    use CloseSshTunnelsTrait;

    public SnowflakeConnection $connection;

    protected string $testProjectDir;

    public ?string $orderResults = null;

    public function __construct(
        ?string $name = null,
        array $data = [],
        string $dataName = '',
    ) {
        putenv('SSH_PRIVATE_KEY=' . file_get_contents('/root/.ssh/id_rsa'));
        putenv('SSH_PUBLIC_KEY=' . file_get_contents('/root/.ssh/id_rsa.pub'));
        parent::__construct($name, $data, $dataName);
        $connectionFactory = new SnowflakeConnectionFactory();
        $databaseConfig = $this->getDatabaseConfig();
        $this->connection = $connectionFactory->create($databaseConfig, new TestLogger());

        $this->connection->exec(sprintf(
            'USE WAREHOUSE %s;',
            $this->connection->quoteIdentifier($databaseConfig->getWarehouse()),
        ));
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->closeSshTunnels();
        $this->dropTables();
        $this->orderResults = null;
        $this->testProjectDir = $this->getTestFileDir() . '/' . $this->dataName();

        // Load setUp.php file - used to init database state
        $setUpPhpFile = $this->testProjectDir . '/setUp.php';
        if (file_exists($setUpPhpFile)) {
            // Get callback from file and check it
            $initCallback = require $setUpPhpFile;
            if (!is_callable($initCallback)) {
                throw new RuntimeException(sprintf('File "%s" must return callback!', $setUpPhpFile));
            }

            // Invoke callback
            $initCallback($this);
        }
    }

    protected function tearDown(): void
    {
        parent::tearDown();

        $setUpPhpFile = $this->testProjectDir . '/tearDown.php';
        if (file_exists($setUpPhpFile)) {
            // Get callback from file and check it
            $initCallback = require $setUpPhpFile;
            if (!is_callable($initCallback)) {
                throw new RuntimeException(sprintf('File "%s" must return callback!', $setUpPhpFile));
            }
            $initCallback($this);
        }
    }

    /**
     * @dataProvider provideDatadirSpecifications
     */
    public function testDatadir(DatadirTestSpecificationInterface $specification): void
    {

        $tempDatadir = $this->getTempDatadir($specification);

        $finder = new Finder();
        $files = $finder
            ->files()
            ->in($this->testProjectDir . '/source/data/in/tables')
            ->name('*.manifest');

        /** @var array<string, array{'stagingStorage': string, 'manifest': array}> $manifestData */
        $manifestData = json_decode(
            (string) file_get_contents(
                __DIR__ . '/../prepare-data/manifestData.json',
            ),
            true,
        );
        foreach ($files as $file) {
            $filename = $file->getFilenameWithoutExtension();
            if (!isset($manifestData[$filename])) {
                throw new RuntimeException(sprintf('Table in storage for file "%s" not found!', $filename));
            }
            $manifestPath = sprintf(
                '%s/in/tables/%s.manifest',
                $tempDatadir->getTmpFolder(),
                $filename,
            );

            $stage = $manifestData[$filename];
            /** @var array<string, array> $manifest */
            $manifest = json_decode((string) file_get_contents($manifestPath), true);
            $manifest[$stage['stagingStorage']] = $stage['manifest'];
            file_put_contents($manifestPath, json_encode($manifest, JSON_PRETTY_PRINT));
        }

        $process = $this->runScript($tempDatadir->getTmpFolder());

        $this->dumpTables($tempDatadir->getTmpFolder());

        $this->assertMatchesSpecification($specification, $process, $tempDatadir->getTmpFolder());
    }

    private function dropTables(): void
    {
        foreach ($this->getTableNames() as $tableName) {
            $this->connection->exec(sprintf(
                'DROP TABLE IF EXISTS %s.%s',
                $this->connection->quoteIdentifier($this->getDatabaseConfig()->getSchema()),
                $this->connection->quoteIdentifier($tableName),
            ));
        }
    }

    /**
     * @throws Exception|InvalidArgumentException
     */
    private function dumpTables(string $tmpFolder): void
    {
        $dumpDir = $tmpFolder . '/out/db-dump';
        $fs = new Filesystem();
        $fs->mkdir($dumpDir);

        foreach ($this->getTableNames() as $tableName) {
            $this->dumpTableData($tableName, $dumpDir);
        }
    }

    private function getTableNames(): array
    {
        $tables = $this->connection->fetchAll(
            sprintf(
                'SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = \'%s\';',
                getenv('DB_SCHEMA'),
            ),
        );

        return array_map(function ($item) {
            return $item['TABLE_NAME'];
        }, $tables);
    }

    /**
     * @throws Exception|InvalidArgumentException
     */
    private function dumpTableData(string $tableName, string $tmpFolder): void
    {
        $csvDumpFile = new CsvFile(sprintf('%s/%s.csv', $tmpFolder, $tableName));
        $sql = sprintf('SELECT * FROM "%s"', $tableName);
        // get primary keys
        $tableInfo = $this->connection->fetchAll(sprintf(
            'DESCRIBE TABLE %s.%s;',
            $this->connection->quoteIdentifier($this->getDatabaseConfig()->getSchema()),
            $this->connection->quoteIdentifier($tableName),
        ));
        /** @var array{'name': string}[] $primaryKeysInDb */
        $primaryKeysInDb = array_filter($tableInfo, fn($v) => $v['primary key'] === 'Y');
        $primaryKeysInDb = array_map(
            fn(array $item) => $this->connection->quoteIdentifier($item['name']),
            $primaryKeysInDb,
        );

        if (!empty($primaryKeysInDb)) {
            $sql .= sprintf(
                ' ORDER BY %s',
                implode(',', $primaryKeysInDb),
            );
        } elseif ($this->orderResults) {
            $sql .= sprintf(
                ' ORDER BY %s',
                $this->connection->quoteIdentifier($this->orderResults),
            );
        }

        $rows = $this->connection->fetchAll($sql);
        if ($rows) {
            $csvDumpFile->writeRow(array_keys(current($rows)));
            foreach ($rows as $row) {
                $row = array_map(fn($v) => is_null($v) ? '' : $v, $row);
                $csvDumpFile->writeRow($row);
            }
        }
    }

    public function getDatabaseConfig(): SnowflakeDatabaseConfig
    {
        $config = [
            'host' =>  getenv('DB_HOST'),
            'port' => getenv('DB_PORT'),
            'database' => getenv('DB_DATABASE'),
            'user' => getenv('DB_USER'),
            'schema' => getenv('DB_SCHEMA'),
            '#password' => getenv('DB_PASSWORD'),
            'warehouse' => getenv('DB_WAREHOUSE'),
        ];

        return SnowflakeDatabaseConfig::fromArray($config);
    }
}
