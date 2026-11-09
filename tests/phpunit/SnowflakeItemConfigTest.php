<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\DbWriter\Configuration\ValueObject\SnowflakeItemConfig;
use PHPUnit\Framework\TestCase;

class SnowflakeItemConfigTest extends TestCase
{
    public function testSizeZeroIsPreserved(): void
    {
        $config = [
            'name' => 'col',
            'dbName' => 'col',
            'type' => 'timestamp_ntz',
            'size' => '0',
            'nullable' => false,
        ];

        $itemConfig = SnowflakeItemConfig::fromArray($config);

        self::assertTrue($itemConfig->hasSize());
        self::assertSame('0', $itemConfig->getSize());
    }

    public function testSizeNonZeroIsPreserved(): void
    {
        $config = [
            'name' => 'col',
            'dbName' => 'col',
            'type' => 'varchar',
            'size' => '255',
            'nullable' => true,
        ];

        $itemConfig = SnowflakeItemConfig::fromArray($config);

        self::assertTrue($itemConfig->hasSize());
        self::assertSame('255', $itemConfig->getSize());
    }

    public function testSizeNullMeansNoSize(): void
    {
        $config = [
            'name' => 'col',
            'dbName' => 'col',
            'type' => 'int',
            'size' => null,
            'nullable' => false,
        ];

        $itemConfig = SnowflakeItemConfig::fromArray($config);

        self::assertFalse($itemConfig->hasSize());
    }

    public function testSizeEmptyStringMeansNoSize(): void
    {
        $config = [
            'name' => 'col',
            'dbName' => 'col',
            'type' => 'int',
            'size' => '',
            'nullable' => false,
        ];

        $itemConfig = SnowflakeItemConfig::fromArray($config);

        self::assertFalse($itemConfig->hasSize());
    }

    public function testDefaultZeroIsPreserved(): void
    {
        $config = [
            'name' => 'col',
            'dbName' => 'col',
            'type' => 'int',
            'size' => null,
            'nullable' => false,
            'default' => '0',
        ];

        $itemConfig = SnowflakeItemConfig::fromArray($config);

        self::assertTrue($itemConfig->hasDefault());
        self::assertSame('0', $itemConfig->getDefault());
    }
}
