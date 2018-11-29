<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake\Tests\Datatype;

use Keboola\DbWriter\Snowflake\DataType\Definition;
use Keboola\DbWriter\Snowflake\Tests\BaseTest;
use Keboola\DbWriter\Writer\Snowflake;

class DefinitionTest extends BaseTest
{
    /** @var Snowflake */
    private $writer;

    private $config;

    public function setUp()
    {
        $this->config = $this->getConfig($this->dataDir);

        $writer = $this->getWriter($this->config['parameters']);
        if ($writer instanceof Snowflake) {
            $this->writer = $writer;
        } else {
            $this->fail('Writer factory must init Snowflake Writer');
        }
    }

    public function timestampBaseTypesData(): array
    {
        return [
            ['TIMESTAMP_LTZ'],
            ['TIMESTAMP_NTZ'],
        ];
    }

    public function definitionFactoryErrorsData(): array
    {
        return [
            [
                [],
                'Missing medata: kind, type, null?, default',
            ],
            [
                [
                    'kind' => 'COLUMN',
                ],
                'Missing medata: type, null?, default',
            ],
            [
                [
                    'kind' => 'COLUMN',
                    'type' => 'TIME',
                ],
                'Missing medata: null?, default',
            ],
            [
                [
                    'kind' => 'COLUMN',
                    'type' => 'TIME',
                    'null?' => 'N',
                ],
                'Missing medata: default',
            ],
            [
                [
                    'kind' => 'UNKNOWN',
                    'type' => 'TIME',
                    'null?' => 'N',
                    'default' => null,
                ],
                'Metadata does not contains column definition',
            ],
        ];
    }

    public function definitionFactoryData(): array
    {
        return [
            [
                'VARCHAR',
                [
                    'length' => 50,
                    'nullable' => true,
                ],
            ],
            [
                'VARCHAR',
                [
                    'length' => 50,
                    'nullable' => true,
                    'default' => null,
                ],
            ],
            [
                'VARCHAR',
                [
                    'length' => 255,
                    'nullable' => false,
                    'default' => 'mydefault',
                ],
            ],
            [
                'VARCHAR',
                [
                    'length' => 255,
                    'nullable' => false,
                    'default' => '',
                ],
            ],
            [
                'TEXT',
                [
                    'nullable' => true,
                    'default' => 'mydefault',
                ],
            ],
            [
                'TEXT',
                [
                    'nullable' => true,
                    'default' => ' ',
                ],
            ],
            [
                'TEXT',
                [
                    'nullable' => true,
                    'default' => '\'',
                ],
            ],
            [
                'TEXT',
                [
                    'nullable' => true,
                    'default' => '\'\'',
                ],
            ],
            [
                'TEXT',
                [
                    'nullable' => true,
                    'default' => '"',
                ],
            ],
            [
                'INT',
                [
                    'nullable' => false,
                    'default' => 60,
                ],
            ],
            [
                'INT',
                [
                    'nullable' => false,
                    'default' => null,
                ],
            ],
            [
                'INT',
                [
                    'nullable' => false,
                    'default' => 0,
                ],
            ],
        ];
    }

    public function writerAllowedTypesData(): array
    {
        return array_map(
            function ($type) {
                return [$type];
            },
            Snowflake::getAllowedTypes()
        );
    }

    public function defaultLengthData(): array
    {
        return array_map(
            function ($type) {
                return [$type];
            },
            \Keboola\Datatype\Definition\Snowflake::TYPES
        );
    }

    public function definitionFactoryFromMappingData(): array
    {
        return [
            [
                [
                    'type' => 'VARCHAR',
                ],
                'VARCHAR',
                null,
                false,
                null,
            ],
            [
                [
                    'type' => 'VARCHAR',
                    'size' => '',
                    'default' => '',
                ],
                'VARCHAR',
                '',
                false,
                '',
            ],
            [
                [
                    'type' => 'VARCHAR',
                    'size' => 123,
                    'default' => ' ',
                ],
                'VARCHAR',
                '123',
                false,
                ' ',
            ],
            [
                [
                    'type' => 'DECIMAL',
                    'size' => '10,2',
                    'nullable' => true,
                    'default' => '123.12',
                ],
                'DECIMAL',
                '10,2',
                true,
                '123.12',
            ],
        ];
    }

    public function stripDefaultValueQuotingData(): array
    {
        return [
            [
                "",
                "",
            ],
            [
                "''",
                "",
            ],
            [
                "' '",
                " ",
            ],
            [
                "'test'",
                "test",
            ],
            [
                "'te''st'",
                "te'st",
            ],
        ];
    }

    /**
     * @dataProvider timestampBaseTypesData
     */
    public function testTimestampBaseTypes(string $baseType): void
    {
        $type = 'TIMESTAMP';

        $this->assertNotEquals($baseType, $type);

        $definition = new Definition($type);
        $this->assertSame($baseType, $definition->getSnowflakeBaseType($baseType));
    }

    /**
     * @dataProvider definitionFactoryErrorsData
     */
    public function testDefinitionFactoryErrors(array $metadata, string $expectedError): void
    {
        try {
            Definition::createFromSnowflakeMetadata($metadata);
            $this->fail('Creating Definition from metadata should fail');
        } catch (\InvalidArgumentException $e) {
            $this->assertSame($expectedError, $e->getMessage());
        }
    }

    /**
     * @dataProvider definitionFactoryData
     */
    public function testDefinitionFactory(string $columnType, array $columnOptions): void
    {
        $timestampTypeMapping = $this->writer->getTimestampTypeMapping();

        $tableName = 'testDefinitionFactory';
        $expectedDefinition = new Definition($columnType, $columnOptions);

        $this->writer->drop($tableName);

        $this->writer->create([
            'dbName' => $tableName,
            'items' => [
                [
                    "name" => "glasses",
                    "dbName" => "test",
                    "type" => strtolower($expectedDefinition->getType()),
                    "size" => $expectedDefinition->getLength(),
                    "nullable" => $expectedDefinition->isNullable(),
                    "default" => $expectedDefinition->getDefault(),
                ],
            ],
        ]);

        $columnsInfo = $this->writer->getTableInfo($tableName);
        $this->assertCount(1, $columnsInfo);

        $columnInfo = reset($columnsInfo);
        $this->assertSame('test', $columnInfo['name']);

        $dbDefinition = Definition::createFromSnowflakeMetadata(reset($columnsInfo));
        if ($expectedDefinition->getLength() === null) {
            $this->assertSame($expectedDefinition->getSnowflakeDefaultLength(), $dbDefinition->getLength());
        } else {
            $this->assertSame($expectedDefinition->getLength(), $dbDefinition->getLength());
        }

        $this->assertSame($expectedDefinition->getDefault(), $dbDefinition->getDefault());
        $this->assertSame($expectedDefinition->isNullable(), $dbDefinition->isNullable());
        $this->assertSame($expectedDefinition->getSnowflakeBaseType($timestampTypeMapping), $dbDefinition->getType());
    }

    /**
     * @dataProvider writerAllowedTypesData
     */
    public function testWriterAllowedTypes($type): void
    {
        $definition = new Definition($type);

        $this->assertSame($type, $definition->getType());
    }

    /**
     * @dataProvider defaultLengthData
     */
    public function testDefaultLength(string $type): void
    {
        $timestampTypeMapping = $this->writer->getTimestampTypeMapping();

        $tableName = 'testDefaultLength';
        $expectedDefinition = new Definition($type, ['nullable' => false]);

        $this->writer->drop($tableName);

        $this->writer->create([
            'dbName' => $tableName,
            'items' => [
                [
                    "name" => "glasses",
                    "dbName" => $tableName,
                    "type" => strtolower($expectedDefinition->getType()),
                    "size" => $expectedDefinition->getLength(),
                    "nullable" => $expectedDefinition->isNullable(),
                    "default" => $expectedDefinition->getDefault(),
                ],
            ],
        ]);

        $columnsInfo = $this->writer->getTableInfo($tableName);
        $this->assertCount(1, $columnsInfo);

        $columnInfo = reset($columnsInfo);
        $this->assertSame($tableName, $columnInfo['name']);

        $dbDefinition = Definition::createFromSnowflakeMetadata(reset($columnsInfo));

        $this->assertSame($expectedDefinition->getSnowflakeDefaultLength(), $dbDefinition->getLength());
        $this->assertSame($expectedDefinition->getDefault(), $dbDefinition->getDefault());
        $this->assertSame($expectedDefinition->isNullable(), $dbDefinition->isNullable());
        $this->assertSame($expectedDefinition->getSnowflakeBaseType($timestampTypeMapping), $dbDefinition->getType());
    }

    /**
     * @dataProvider definitionFactoryFromMappingData
     */
    public function testDefinitionFactoryFromMapping(array $column, string $exType, $exSize, bool $exNullable, $exDefault): void
    {
        $expectedDefinition = new Definition($exType, [
            "nullable" => $exNullable,
            "length" => $exSize,
            "default" => $exDefault,
        ]);

        $mappingDefinition = Definition::createFromTableMapping($column);

        $this->assertSame($expectedDefinition->getType(), $mappingDefinition->getType());
        $this->assertSame($expectedDefinition->getLength(), $mappingDefinition->getLength());
        $this->assertSame($expectedDefinition->isNullable(), $mappingDefinition->isNullable());
        $this->assertSame($expectedDefinition->getDefault(), $mappingDefinition->getDefault());
    }

    /**
     * @dataProvider stripDefaultValueQuotingData
     */
    public function testStripDefaultValueQuoting(string $input, string $output): void
    {
        $this->assertSame(Definition::stripDefaultValueQuoting($input), $output);
    }
}
