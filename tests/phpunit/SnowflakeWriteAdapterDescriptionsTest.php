<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\DbWriter\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbWriter\Writer\SnowflakeConnection;
use Keboola\DbWriter\Writer\SnowflakeQueryBuilder;
use Keboola\DbWriter\Writer\SnowflakeWriteAdapter;
use Keboola\DbWriterAdapter\ValueObject\TableDescriptions;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class SnowflakeWriteAdapterDescriptionsTest extends TestCase
{
    /** @var string[] */
    private array $executed = [];

    private SnowflakeWriteAdapter $adapter;

    protected function setUp(): void
    {
        $this->executed = [];
        $connection = $this->createPartialMock(SnowflakeConnection::class, ['exec']);
        $connection->method('exec')->willReturnCallback(function (string $sql): void {
            $this->executed[] = $sql;
        });

        $databaseConfig = new SnowflakeDatabaseConfig(
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

        $this->adapter = new SnowflakeWriteAdapter(
            $connection,
            new SnowflakeQueryBuilder($databaseConfig),
            new TestLogger(),
        );
    }

    public function testTableAndColumns(): void
    {
        $this->adapter->applyDescriptions('simple', new TableDescriptions(
            'Table description',
            ['id' => 'Identifier', 'name' => 'Full name'],
        ));

        self::assertSame([
            'COMMENT ON TABLE "simple" IS \'Table description\'',
            'COMMENT ON COLUMN "simple"."id" IS \'Identifier\'',
            'COMMENT ON COLUMN "simple"."name" IS \'Full name\'',
        ], $this->executed);
    }

    public function testColumnsOnly(): void
    {
        $this->adapter->applyDescriptions('simple', new TableDescriptions(null, ['id' => 'Identifier']));

        self::assertSame([
            'COMMENT ON COLUMN "simple"."id" IS \'Identifier\'',
        ], $this->executed);
    }

    public function testEmptyTableDescriptionIsSkipped(): void
    {
        $this->adapter->applyDescriptions('simple', new TableDescriptions('', []));

        self::assertSame([], $this->executed);
    }

    public function testQuoting(): void
    {
        $this->adapter->applyDescriptions('my "table"', new TableDescriptions(
            'It\'s a "table" with \\ backslash',
            ['col "x"' => 'Bob\'s column'],
        ));

        self::assertSame([
            'COMMENT ON TABLE "my ""table""" IS \'It\\\'s a \\"table\\" with \\\\ backslash\'',
            'COMMENT ON COLUMN "my ""table"""."col ""x""" IS \'Bob\\\'s column\'',
        ], $this->executed);
    }

    public function testNumericColumnName(): void
    {
        // PHP casts numeric string keys to int at runtime.
        /** @var array<string, string> $columns */
        $columns = ['123' => 'Numeric name'];
        $this->adapter->applyDescriptions('simple', new TableDescriptions(null, $columns));

        self::assertSame([
            'COMMENT ON COLUMN "simple"."123" IS \'Numeric name\'',
        ], $this->executed);
    }
}
