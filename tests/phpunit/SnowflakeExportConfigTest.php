<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\DbWriter\Configuration\ValueObject\SnowflakeExportConfig;
use PHPUnit\Framework\TestCase;

class SnowflakeExportConfigTest extends TestCase
{
    public function testPropagateDescriptionsDefaultsToTrue(): void
    {
        $exportConfig = SnowflakeExportConfig::fromArray($this->getConfig(), $this->getInputMapping());

        self::assertTrue($exportConfig->shouldPropagateDescriptions());
    }

    public function testPropagateDescriptionsFalseIsPassedThrough(): void
    {
        $config = $this->getConfig();
        $config['propagateDescriptions'] = false;

        $exportConfig = SnowflakeExportConfig::fromArray($config, $this->getInputMapping());

        self::assertFalse($exportConfig->shouldPropagateDescriptions());
    }

    /**
     * @return array<string, mixed>
     */
    private function getConfig(): array
    {
        return [
            'data_dir' => '/data',
            'writer_class' => 'Snowflake',
            'db' => [
                'host' => 'host',
                'database' => 'database',
                'user' => 'user',
                '#password' => 'password',
                'schema' => 'schema',
            ],
            'tableId' => 'in.c-main.table',
            'dbName' => 'table',
            'items' => [
                ['name' => 'id', 'dbName' => 'id', 'type' => 'int', 'size' => '', 'nullable' => false],
            ],
        ];
    }

    /**
     * @return array<int, array<string, mixed>>
     */
    private function getInputMapping(): array
    {
        return [['source' => 'in.c-main.table', 'destination' => 'table.csv', 'columns' => ['id']]];
    }
}
