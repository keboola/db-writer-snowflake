<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake\Tests;

use Generator;
use Keboola\DbWriter\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbWriter\Writer\SnowflakeQueryBuilder;
use Keboola\DbWriterAdapter\Connection\Connection;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class SnowflakeQueryBuilderTest extends TestCase
{
    private SnowflakeQueryBuilder $queryBuilder;

    /** @var Connection&MockObject */
    private $connection;

    private SnowflakeDatabaseConfig $databaseConfig;

    protected function setUp(): void
    {
        $this->databaseConfig = new SnowflakeDatabaseConfig(
            'localhost',
            '443',
            'TEST_DB',
            'TEST_WH',
            null,
            null,
            'user',
            'password',
            null,
            'TEST_SCHEMA',
            null,
            null,
        );

        $this->queryBuilder = new SnowflakeQueryBuilder($this->databaseConfig);

        $this->connection = $this->createMock(Connection::class);
        $this->connection->expects($this->any())
            ->method('quoteIdentifier')
            ->willReturnCallback(fn(string $str): string => sprintf('"%s"', $str));
        $this->connection->expects($this->any())
            ->method('quote')
            ->willReturnCallback(fn(string $str): string => sprintf("'%s'", $str));
    }

    public function testCreateQueryStatementTempTable(): void
    {
        $items = [
            $this->createItemConfig('col1', 'varchar', '255', false, null),
        ];

        $result = $this->queryBuilder->createQueryStatement(
            $this->connection,
            'test_table',
            true,
            $items,
        );

        self::assertSame(
            'CREATE TEMPORARY TABLE "test_table" ("col1" VARCHAR(255) NOT NULL )',
            $result,
        );
    }

    public function testCreateQueryStatementNonTempTable(): void
    {
        $items = [
            $this->createItemConfig('col1', 'varchar', '255', false, null),
        ];

        $result = $this->queryBuilder->createQueryStatement(
            $this->connection,
            'test_table',
            false,
            $items,
        );

        self::assertSame(
            'CREATE TABLE IF NOT EXISTS "test_table" ("col1" VARCHAR(255) NOT NULL )',
            $result,
        );
    }

    public function testCreateQueryStatementWithPrimaryKeys(): void
    {
        $items = [
            $this->createItemConfig('col1', 'varchar', '255', false, null),
            $this->createItemConfig('col2', 'int', null, false, null),
        ];

        $result = $this->queryBuilder->createQueryStatement(
            $this->connection,
            'test_table',
            false,
            $items,
            ['col1', 'col2'],
        );

        self::assertSame(
            'CREATE TABLE IF NOT EXISTS "test_table"'
            . ' ("col1" VARCHAR(255) NOT NULL , "col2" INT NOT NULL , PRIMARY KEY("col1", "col2"))',
            $result,
        );
    }

    public function testCreateQueryStatementWithoutPrimaryKeys(): void
    {
        $items = [
            $this->createItemConfig('col1', 'varchar', '255', false, null),
        ];

        $result = $this->queryBuilder->createQueryStatement(
            $this->connection,
            'test_table',
            false,
            $items,
            null,
        );

        self::assertSame(
            'CREATE TABLE IF NOT EXISTS "test_table" ("col1" VARCHAR(255) NOT NULL )',
            $result,
        );
    }

    /**
     * @dataProvider ignoreTypeDataProvider
     */
    public function testCreateQueryStatementSkipsIgnoredItems(string $ignoreType): void
    {
        $items = [
            $this->createItemConfig('col1', 'varchar', '255', false, null),
            $this->createItemConfig('ignored_col', $ignoreType, null, false, null),
        ];

        $result = $this->queryBuilder->createQueryStatement(
            $this->connection,
            'test_table',
            true,
            $items,
        );

        self::assertSame(
            'CREATE TEMPORARY TABLE "test_table" ("col1" VARCHAR(255) NOT NULL )',
            $result,
        );
    }

    public static function ignoreTypeDataProvider(): Generator
    {
        yield 'lowercase' => ['ignore'];
        yield 'uppercase' => ['IGNORE'];
        yield 'capitalized' => ['Ignore'];
        yield 'mixed case' => ['iGnOrE'];
    }

    /**
     * @dataProvider typesWithSizeDataProvider
     */
    public function testCreateQueryStatementTypesWithSize(
        string $type,
        string $size,
        string $expectedQuery,
    ): void {
        $items = [
            $this->createItemConfig('col1', $type, $size, false, null),
        ];

        $result = $this->queryBuilder->createQueryStatement(
            $this->connection,
            'test_table',
            true,
            $items,
        );

        self::assertSame($expectedQuery, $result);
    }

    public static function typesWithSizeDataProvider(): Generator
    {
        // Types in TYPES_WITH_SIZE (all lowercase) — size applied
        yield 'number' => [
            'type' => 'number',
            'size' => '38,0',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" NUMBER(38,0) NOT NULL )',
        ];
        yield 'decimal' => [
            'type' => 'decimal',
            'size' => '10,2',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" DECIMAL(10,2) NOT NULL )',
        ];
        yield 'numeric' => [
            'type' => 'numeric',
            'size' => '10,2',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" NUMERIC(10,2) NOT NULL )',
        ];
        yield 'char' => [
            'type' => 'char',
            'size' => '10',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" CHAR(10) NOT NULL )',
        ];
        yield 'character' => [
            'type' => 'character',
            'size' => '10',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" CHARACTER(10) NOT NULL )',
        ];
        yield 'varchar' => [
            'type' => 'varchar',
            'size' => '255',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" VARCHAR(255) NOT NULL )',
        ];
        yield 'string' => [
            'type' => 'string',
            'size' => '100',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" STRING(100) NOT NULL )',
        ];
        yield 'text' => [
            'type' => 'text',
            'size' => '1000',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" TEXT(1000) NOT NULL )',
        ];
        yield 'binary' => [
            'type' => 'binary',
            'size' => '100',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" BINARY(100) NOT NULL )',
        ];
        yield 'time' => [
            'type' => 'time',
            'size' => '9',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" TIME(9) NOT NULL )',
        ];
        yield 'datetime' => [
            'type' => 'datetime',
            'size' => '6',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" DATETIME(6) NOT NULL )',
        ];
        yield 'timestamp' => [
            'type' => 'timestamp',
            'size' => '9',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" TIMESTAMP(9) NOT NULL )',
        ];
        yield 'timestamp_ntz' => [
            'type' => 'timestamp_ntz',
            'size' => '9',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" TIMESTAMP_NTZ(9) NOT NULL )',
        ];
        yield 'timestamp_ltz' => [
            'type' => 'timestamp_ltz',
            'size' => '9',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" TIMESTAMP_LTZ(9) NOT NULL )',
        ];
        yield 'timestamp_tz' => [
            'type' => 'timestamp_tz',
            'size' => '3',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" TIMESTAMP_TZ(3) NOT NULL )',
        ];
        yield 'TIMESTAMP_NTZ uppercase' => [
            'type' => 'TIMESTAMP_NTZ',
            'size' => '9',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" TIMESTAMP_NTZ(9) NOT NULL )',
        ];
        // Types NOT in TYPES_WITH_SIZE — size not applied
        yield 'int' => [
            'type' => 'int',
            'size' => '11',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" INT NOT NULL )',
        ];
        yield 'float' => [
            'type' => 'float',
            'size' => '10',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" FLOAT NOT NULL )',
        ];
        yield 'boolean' => [
            'type' => 'boolean',
            'size' => '1',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" BOOLEAN NOT NULL )',
        ];
        yield 'date' => [
            'type' => 'date',
            'size' => '10',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" DATE NOT NULL )',
        ];
        // Case-insensitive: uppercase VARCHAR now correctly matches — size applied
        yield 'VARCHAR uppercase' => [
            'type' => 'VARCHAR',
            'size' => '255',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" VARCHAR(255) NOT NULL )',
        ];
    }

    public function testCreateQueryStatementItemWithNoSize(): void
    {
        $items = [
            $this->createItemConfig('col1', 'varchar', null, false, null),
        ];

        $result = $this->queryBuilder->createQueryStatement(
            $this->connection,
            'test_table',
            true,
            $items,
        );

        self::assertSame(
            'CREATE TEMPORARY TABLE "test_table" ("col1" VARCHAR NOT NULL )',
            $result,
        );
    }

    /**
     * @dataProvider noSizeTimestampDataProvider
     */
    public function testCreateQueryStatementTimestampWithoutSize(
        string $type,
        string $expectedQuery,
    ): void {
        $items = [
            $this->createItemConfig('col1', $type, null, false, null),
        ];

        $result = $this->queryBuilder->createQueryStatement(
            $this->connection,
            'test_table',
            true,
            $items,
        );

        self::assertSame($expectedQuery, $result);
    }

    public static function noSizeTimestampDataProvider(): Generator
    {
        yield 'time without size' => [
            'type' => 'time',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" TIME NOT NULL )',
        ];
        yield 'datetime without size' => [
            'type' => 'datetime',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" DATETIME NOT NULL )',
        ];
        yield 'timestamp without size' => [
            'type' => 'timestamp',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" TIMESTAMP NOT NULL )',
        ];
        yield 'timestamp_ntz without size' => [
            'type' => 'timestamp_ntz',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" TIMESTAMP_NTZ NOT NULL )',
        ];
        yield 'timestamp_ltz without size' => [
            'type' => 'timestamp_ltz',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" TIMESTAMP_LTZ NOT NULL )',
        ];
        yield 'timestamp_tz without size' => [
            'type' => 'timestamp_tz',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" TIMESTAMP_TZ NOT NULL )',
        ];
    }

    /**
     * @dataProvider nullableDataProvider
     */
    public function testCreateQueryStatementNullable(bool $nullable, string $expectedQuery): void
    {
        $items = [
            $this->createItemConfig('col1', 'varchar', '255', $nullable, null),
        ];

        $result = $this->queryBuilder->createQueryStatement(
            $this->connection,
            'test_table',
            true,
            $items,
        );

        self::assertSame($expectedQuery, $result);
    }

    public static function nullableDataProvider(): Generator
    {
        yield 'nullable column' => [
            true,
            'CREATE TEMPORARY TABLE "test_table" ("col1" VARCHAR(255) NULL )',
        ];
        yield 'not nullable column' => [
            false,
            'CREATE TEMPORARY TABLE "test_table" ("col1" VARCHAR(255) NOT NULL )',
        ];
    }

    /**
     * @dataProvider defaultValueDataProvider
     */
    public function testCreateQueryStatementDefault(
        string $type,
        ?string $default,
        string $expectedQuery,
    ): void {
        $items = [
            $this->createItemConfig('col1', $type, null, false, $default),
        ];

        $result = $this->queryBuilder->createQueryStatement(
            $this->connection,
            'test_table',
            true,
            $items,
        );

        self::assertSame($expectedQuery, $result);
    }

    public static function defaultValueDataProvider(): Generator
    {
        yield 'varchar with default' => [
            'type' => 'varchar',
            'default' => 'test_value',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table"'
                . ' ("col1" VARCHAR NOT NULL DEFAULT CAST(\'test_value\' AS VARCHAR))',
        ];
        yield 'int with default' => [
            'type' => 'int',
            'default' => '42',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table"'
                . ' ("col1" INT NOT NULL DEFAULT CAST(\'42\' AS INT))',
        ];
        yield 'TEXT type excluded' => [
            'type' => 'TEXT',
            'default' => 'value',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" TEXT NOT NULL )',
        ];
        yield 'no default value' => [
            'type' => 'varchar',
            'default' => null,
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" VARCHAR NOT NULL )',
        ];
        yield 'lowercase text excluded (case-insensitive)' => [
            'type' => 'text',
            'default' => 'value',
            'expectedQuery' => 'CREATE TEMPORARY TABLE "test_table" ("col1" TEXT NOT NULL )',
        ];
    }

    public function testCreateQueryStatementComplexMultipleItems(): void
    {
        $items = [
            $this->createItemConfig('id', 'number', '38,0', false, null),
            $this->createItemConfig('name', 'varchar', '255', true, 'unknown'),
            $this->createItemConfig('description', 'text', '1000', true, null),
            $this->createItemConfig('temp_col', 'ignore', null, false, null),
            $this->createItemConfig('active', 'boolean', null, false, null),
        ];

        $result = $this->queryBuilder->createQueryStatement(
            $this->connection,
            'my_table',
            false,
            $items,
            ['id'],
        );

        self::assertSame(
            'CREATE TABLE IF NOT EXISTS "my_table"'
            . ' ("id" NUMBER(38,0) NOT NULL '
            . ', "name" VARCHAR(255) NULL DEFAULT CAST(\'unknown\' AS VARCHAR)'
            . ', "description" TEXT(1000) NULL '
            . ', "active" BOOLEAN NOT NULL '
            . ', PRIMARY KEY("id"))',
            $result,
        );
    }

    public function testDropStageStatement(): void
    {
        $result = $this->queryBuilder->dropStageStatement($this->connection, 'my_stage');

        self::assertSame('DROP STAGE IF EXISTS "my_stage"', $result);
    }

    public function testTableExistsQueryStatement(): void
    {
        $result = $this->queryBuilder->tableExistsQueryStatement($this->connection, 'my_table');

        // Source uses multiline sprintf template preserving internal whitespace
        $expected = '
                SELECT *
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = \'my_table\'
                AND TABLE_SCHEMA = \'TEST_SCHEMA\'
                AND TABLE_CATALOG = \'TEST_DB\'
            ';
        self::assertSame($expected, $result);
    }

    public function testTableInfoQueryStatement(): void
    {
        $result = $this->queryBuilder->tableInfoQueryStatement($this->connection, 'my_table');

        self::assertSame('DESCRIBE TABLE "TEST_SCHEMA"."my_table"', $result);
    }

    public function testDescribeTableColumnsQueryStatement(): void
    {
        $result = $this->queryBuilder->describeTableColumnsQueryStatement(
            $this->connection,
            'my_table',
        );

        self::assertSame('SHOW COLUMNS IN "TEST_SCHEMA"."my_table"', $result);
    }

    public function testAddUniqueKeyQueryStatement(): void
    {
        $result = $this->queryBuilder->addUniqueKeyQueryStatement(
            $this->connection,
            'my_table',
            'unique_col',
        );

        self::assertSame(
            'ALTER TABLE "TEST_SCHEMA"."my_table" ADD UNIQUE ("unique_col")',
            $result,
        );
    }

    /**
     * @dataProvider primaryKeyDataProvider
     */
    public function testAddPrimaryKeyQueryStatement(array $primaryKeys, string $expectedQuery): void
    {
        $result = $this->queryBuilder->addPrimaryKeyQueryStatement(
            $this->connection,
            'my_table',
            $primaryKeys,
        );

        self::assertSame($expectedQuery, $result);
    }

    public static function primaryKeyDataProvider(): Generator
    {
        yield 'single primary key' => [
            ['id'],
            'ALTER TABLE "TEST_SCHEMA"."my_table" ADD PRIMARY KEY("id")',
        ];
        yield 'composite primary key' => [
            ['id', 'name'],
            'ALTER TABLE "TEST_SCHEMA"."my_table" ADD PRIMARY KEY("id", "name")',
        ];
    }

    public function testAddForeignKeyQueryStatement(): void
    {
        $result = $this->queryBuilder->addForeignKeyQueryStatement(
            $this->connection,
            'orders',
            'customer_id',
            'customers',
            'id',
        );

        self::assertSame(
            'ALTER TABLE "TEST_SCHEMA"."orders"'
            . ' ADD CONSTRAINT FK_customers_id'
            . ' FOREIGN KEY ("customer_id")'
            . ' REFERENCES "TEST_SCHEMA"."customers"("id")',
            $result,
        );
    }

    public function testUpsertUpdateRowsQueryStatement(): void
    {
        $exportConfig = $this->createExportConfig(
            'target_table',
            ['pk1', 'pk2'],
            [
                ['dbName' => 'pk1'],
                ['dbName' => 'pk2'],
                ['dbName' => 'col1'],
            ],
        );

        $result = $this->queryBuilder->upsertUpdateRowsQueryStatement(
            $this->connection,
            $exportConfig,
            'stage_table',
        );

        self::assertSame(
            'UPDATE "TEST_SCHEMA"."target_table"'
            . ' SET "pk1" = "TEST_SCHEMA"."stage_table"."pk1"'
            . ',"pk2" = "TEST_SCHEMA"."stage_table"."pk2"'
            . ',"col1" = "TEST_SCHEMA"."stage_table"."col1"'
            . ' FROM "TEST_SCHEMA"."stage_table"'
            . ' WHERE "TEST_SCHEMA"."target_table"."pk1" = "TEST_SCHEMA"."stage_table"."pk1"'
            . ' AND "TEST_SCHEMA"."target_table"."pk2" = "TEST_SCHEMA"."stage_table"."pk2";',
            $result,
        );
    }

    public function testUpsertUpdateRowsQueryStatementSinglePrimaryKey(): void
    {
        $exportConfig = $this->createExportConfig(
            'target_table',
            ['pk1'],
            [
                ['dbName' => 'pk1'],
                ['dbName' => 'col1'],
            ],
        );

        $result = $this->queryBuilder->upsertUpdateRowsQueryStatement(
            $this->connection,
            $exportConfig,
            'stage_table',
        );

        self::assertSame(
            'UPDATE "TEST_SCHEMA"."target_table"'
            . ' SET "pk1" = "TEST_SCHEMA"."stage_table"."pk1"'
            . ',"col1" = "TEST_SCHEMA"."stage_table"."col1"'
            . ' FROM "TEST_SCHEMA"."stage_table"'
            . ' WHERE "TEST_SCHEMA"."target_table"."pk1" = "TEST_SCHEMA"."stage_table"."pk1";',
            $result,
        );
    }

    public function testUpsertDeleteRowsQueryStatement(): void
    {
        $exportConfig = $this->createExportConfig(
            'target_table',
            ['pk1', 'pk2'],
            [
                ['dbName' => 'pk1'],
                ['dbName' => 'pk2'],
                ['dbName' => 'col1'],
            ],
        );

        $result = $this->queryBuilder->upsertDeleteRowsQueryStatement(
            $this->connection,
            $exportConfig,
            'stage_table',
        );

        self::assertSame(
            'DELETE FROM "TEST_SCHEMA"."stage_table"'
            . ' USING "TEST_SCHEMA"."target_table"'
            . ' WHERE "TEST_SCHEMA"."target_table"."pk1" = "TEST_SCHEMA"."stage_table"."pk1"'
            . ' AND "TEST_SCHEMA"."target_table"."pk2" = "TEST_SCHEMA"."stage_table"."pk2"',
            $result,
        );
    }

    public function testUpsertDeleteRowsQueryStatementSinglePrimaryKey(): void
    {
        $exportConfig = $this->createExportConfig(
            'target_table',
            ['pk1'],
            [
                ['dbName' => 'pk1'],
                ['dbName' => 'col1'],
            ],
        );

        $result = $this->queryBuilder->upsertDeleteRowsQueryStatement(
            $this->connection,
            $exportConfig,
            'stage_table',
        );

        self::assertSame(
            'DELETE FROM "TEST_SCHEMA"."stage_table"'
            . ' USING "TEST_SCHEMA"."target_table"'
            . ' WHERE "TEST_SCHEMA"."target_table"."pk1" = "TEST_SCHEMA"."stage_table"."pk1"',
            $result,
        );
    }

    private function createItemConfig(
        string $dbName,
        string $type,
        ?string $size,
        bool $nullable,
        ?string $default,
    ): ItemConfig {
        $config = [
            'name' => $dbName,
            'dbName' => $dbName,
            'type' => $type,
            'nullable' => $nullable,
        ];

        if ($size !== null) {
            $config['size'] = $size;
        }

        if ($default !== null) {
            $config['default'] = $default;
        }

        return ItemConfig::fromArray($config);
    }

    /**
     * @param array<array{dbName: string, type?: string, size?: string}> $itemDefinitions
     */
    private function createExportConfig(
        string $dbName,
        array $primaryKey,
        array $itemDefinitions,
    ): ExportConfig {
        $items = [];
        foreach ($itemDefinitions as $def) {
            $items[] = [
                'name' => $def['dbName'],
                'dbName' => $def['dbName'],
                'type' => $def['type'] ?? 'varchar',
                'size' => $def['size'] ?? '255',
            ];
        }

        return ExportConfig::fromArray(
            [
                'data_dir' => '/tmp',
                'writer_class' => 'Snowflake',
                'tableId' => 'in.c-main.test',
                'dbName' => $dbName,
                'primaryKey' => $primaryKey,
                'items' => $items,
            ],
            [
                ['source' => 'in.c-main.test', 'destination' => 'test.csv'],
            ],
            $this->databaseConfig,
        );
    }
}
