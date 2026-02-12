<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\DbWriter\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbWriter\Writer\SnowflakeQueryBuilder;
use Keboola\DbWriterAdapter\Connection\Connection;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class SnowflakeQueryBuilderTest extends TestCase
{
    private function createQueryBuilder(): SnowflakeQueryBuilder
    {
        $databaseConfig = SnowflakeDatabaseConfig::fromArray([
            'host' => 'test.snowflakecomputing.com',
            'port' => '443',
            'database' => 'testdb',
            'schema' => 'testschema',
            'user' => 'testuser',
            '#password' => 'testpass',
            'warehouse' => 'testwarehouse',
        ]);

        return new SnowflakeQueryBuilder($databaseConfig);
    }

    private function createConnectionMock(): Connection
    {
        $connection = $this->createMock(Connection::class);
        $connection->method('quoteIdentifier')
            ->willReturnCallback(fn(string $str) => sprintf('"%s"', $str));
        $connection->method('quote')
            ->willReturnCallback(fn(string $str) => sprintf("'%s'", $str));

        return $connection;
    }

    /**
     * @dataProvider typeCaseProvider
     */
    public function testSizeIsAppendedRegardlessOfTypeCase(
        string $type,
        string $expectedTypeInSql,
        string $expectedSizeClause,
    ): void {
        $queryBuilder = $this->createQueryBuilder();
        $connection = $this->createConnectionMock();

        $items = [
            ItemConfig::fromArray([
                'name' => 'col1',
                'dbName' => 'col1',
                'type' => $type,
                'size' => '255',
                'nullable' => false,
                'default' => null,
            ]),
        ];

        $sql = $queryBuilder->createQueryStatement($connection, 'test_table', false, $items);

        Assert::assertStringContainsString(
            sprintf('%s%s', $expectedTypeInSql, $expectedSizeClause),
            $sql,
            sprintf('Type "%s" should produce "%s%s" in SQL', $type, $expectedTypeInSql, $expectedSizeClause),
        );
    }

    public function typeCaseProvider(): array
    {
        return [
            'lowercase varchar' => ['varchar', 'VARCHAR', '(255)'],
            'uppercase VARCHAR' => ['VARCHAR', 'VARCHAR', '(255)'],
            'mixed case Varchar' => ['Varchar', 'VARCHAR', '(255)'],
            'lowercase number' => ['number', 'NUMBER', '(255)'],
            'uppercase NUMBER' => ['NUMBER', 'NUMBER', '(255)'],
            'lowercase text' => ['text', 'TEXT', '(255)'],
            'uppercase TEXT' => ['TEXT', 'TEXT', '(255)'],
            'lowercase char' => ['char', 'CHAR', '(255)'],
            'uppercase CHAR' => ['CHAR', 'CHAR', '(255)'],
            'lowercase binary' => ['binary', 'BINARY', '(255)'],
            'uppercase BINARY' => ['BINARY', 'BINARY', '(255)'],
            'lowercase decimal' => ['decimal', 'DECIMAL', '(255)'],
            'uppercase DECIMAL' => ['DECIMAL', 'DECIMAL', '(255)'],
        ];
    }

    public function testSizeNotAppendedForNonSizedTypes(): void
    {
        $queryBuilder = $this->createQueryBuilder();
        $connection = $this->createConnectionMock();

        $items = [
            ItemConfig::fromArray([
                'name' => 'col1',
                'dbName' => 'col1',
                'type' => 'INT',
                'size' => '11',
                'nullable' => false,
                'default' => null,
            ]),
        ];

        $sql = $queryBuilder->createQueryStatement($connection, 'test_table', false, $items);

        Assert::assertStringContainsString('INT NOT NULL', $sql);
        Assert::assertStringNotContainsString('INT(11)', $sql);
    }

    /**
     * @dataProvider textDefaultProvider
     */
    public function testDefaultExcludedForTextTypeRegardlessOfCase(string $textType): void
    {
        $queryBuilder = $this->createQueryBuilder();
        $connection = $this->createConnectionMock();

        $items = [
            ItemConfig::fromArray([
                'name' => 'col1',
                'dbName' => 'col1',
                'type' => $textType,
                'size' => '255',
                'nullable' => false,
                'default' => 'some_default',
            ]),
        ];

        $sql = $queryBuilder->createQueryStatement($connection, 'test_table', false, $items);

        Assert::assertStringNotContainsString(
            'DEFAULT',
            $sql,
            sprintf('Type "%s" should NOT have a DEFAULT clause', $textType),
        );
    }

    public function textDefaultProvider(): array
    {
        return [
            'uppercase TEXT' => ['TEXT'],
            'lowercase text' => ['text'],
            'mixed case Text' => ['Text'],
            'mixed case tEXT' => ['tEXT'],
        ];
    }

    public function testDefaultIncludedForNonTextType(): void
    {
        $queryBuilder = $this->createQueryBuilder();
        $connection = $this->createConnectionMock();

        $items = [
            ItemConfig::fromArray([
                'name' => 'col1',
                'dbName' => 'col1',
                'type' => 'VARCHAR',
                'size' => '255',
                'nullable' => false,
                'default' => 'some_default',
            ]),
        ];

        $sql = $queryBuilder->createQueryStatement($connection, 'test_table', false, $items);

        Assert::assertStringContainsString('DEFAULT CAST(\'some_default\' AS varchar)', $sql);
    }

    /**
     * @dataProvider ignoreCaseProvider
     */
    public function testIgnoreTypeSkippedRegardlessOfCase(string $ignoreType): void
    {
        $queryBuilder = $this->createQueryBuilder();
        $connection = $this->createConnectionMock();

        $items = [
            ItemConfig::fromArray([
                'name' => 'col1',
                'dbName' => 'col1',
                'type' => 'varchar',
                'size' => '255',
                'nullable' => false,
                'default' => null,
            ]),
            ItemConfig::fromArray([
                'name' => 'col2',
                'dbName' => 'col2',
                'type' => $ignoreType,
                'size' => null,
                'nullable' => false,
                'default' => null,
            ]),
        ];

        $sql = $queryBuilder->createQueryStatement($connection, 'test_table', false, $items);

        Assert::assertStringContainsString('"col1"', $sql);
        Assert::assertStringNotContainsString('"col2"', $sql);
    }

    public function ignoreCaseProvider(): array
    {
        return [
            'lowercase ignore' => ['ignore'],
            'uppercase IGNORE' => ['IGNORE'],
            'mixed case Ignore' => ['Ignore'],
        ];
    }
}
