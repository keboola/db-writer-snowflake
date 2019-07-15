<?php

use Keboola\DbWriter\Snowflake\Tests\BaseTest;
use Keboola\DbWriter\Logger;

class SqlTest extends BaseTest
{
    public function testGetColumnSqlDefinition(array $table, string $expectedSQL): void
    {
        $config = parent::getConfig();
        $writer = new Keboola\DbWriter\Writer\Snowflake($config['parameters']['db'], new Logger());

        $columnSqlDefinition = $writer->getColumnsSqlDefinition($table);

        $this->assertEquals($expectedSQL, $columnSqlDefinition);
    }

    public function TableColumnsProvider(): array
    {
        return [
            [
                'not null text column with default' => [
                    [
                        "name" => "text_column",
                        "type" => "TEXT",
                        "nullable" => false,
                        "default" => "test text"
                    ],
                    "text_column TEXT NOT NULL DEFAULT 'test text'"
                ],
                'not null numeric column with default' => [
                    [
                        "name" => "numeric_column",
                        "type" => "NUMBER",
                        "nullable" => false,
                        "default" => 42.2
                    ],
                    "numeric_column NUMBER NOT NULL DEFAULT 42.2"
                ],
                'not null integer column with default' => [
                    [
                        "name" => "integer_column",
                        "type" => "INTEGER",
                        "nullable" => false,
                        "default" => 42
                    ],
                    "integer_column INTEGER NOT NULL DEFAULT 42"
                ],
                'null text column no default' => [
                    [
                        "name" => "text_column",
                        "type" => "TEXT",
                        "nullable" => true,
                        "default" => null
                    ]
                ]
            ]
        ];
    }
}
