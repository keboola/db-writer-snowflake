<?php

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

    public function timestampBaseTypes()
    {
        return [
            ['TIMESTAMP_LTZ'],
            ['TIMESTAMP_NTZ'],
        ];
    }

    public function definitionFactoryErrors(): array
    {
        return [
            [
                [],
                'Missing medata: kind, type, null?, default'
            ],
            [
                [
                    'kind' => 'COLUMN',
                ],
                'Missing medata: type, null?, default'
            ],
            [
                [
                    'kind' => 'COLUMN',
                    'type' => 'TIME',
                ],
                'Missing medata: null?, default'
            ],
            [
                [
                    'kind' => 'COLUMN',
                    'type' => 'TIME',
                    'null?' => 'N',
                ],
                'Missing medata: default'
            ],
            [
                [
                    'kind' => 'UNKNOWN',
                    'type' => 'TIME',
                    'null?' => 'N',
                    'default' => null,
                ],
                'Metadata does not contains column definition'
            ],
        ];
    }

    public function writerAllowedTypes(): array
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

    /**
     * @dataProvider timestampBaseTypes
     */
    public function testTimestampBaseTypes(string $baseType)
    {
        $type = 'TIMESTAMP';

        $this->assertNotEquals($baseType, $type);

        $definition = new Definition($type);
        $this->assertEquals($baseType, $definition->getSnowflakeBaseType($baseType));
    }

    /**
     * @dataProvider definitionFactoryErrors
     */
    public function testDefinitionFactoryErrors(array $metadata, string $expectedError)
    {
        try {
            Definition::createFromSnowflakeMetadata($metadata);
            $this->fail('Creating Definition from metadata should fail');
        } catch (\InvalidArgumentException $e) {
            $this->assertEquals($expectedError, $e->getMessage());

        }
    }

    public function definitionFactory(): array
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
//            [
//                // @FIXME after https://github.com/keboola/db-writer-snowflake/issues/50
//                'INT',
//                [
//                    'nullable' => false,
//                    'default' => 60,
//                ]
//            ],
        ];
    }

    /**
     * @dataProvider definitionFactory
     */
    public function testDefinitionFactory(string $columnType, array $columnOptions)
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
                ]
            ]
        ]);

        $columnsInfo = $this->writer->getTableInfo($tableName);
        $this->assertCount(1, $columnsInfo);

        $columnInfo = reset($columnsInfo);
        $this->assertEquals('test', $columnInfo['name']);

        $dbDefinition = Definition::createFromSnowflakeMetadata(reset($columnsInfo));

        if ($expectedDefinition->getLength() === null) {
            $this->assertEquals($expectedDefinition->getSnowflakeDefaultLength(), $dbDefinition->getLength());
        } else {
            $this->assertEquals($expectedDefinition->getLength(), $dbDefinition->getLength());
        }

        if ($expectedDefinition->getBasetype() === 'STRING' && $expectedDefinition->getDefault() !== null) {
            $this->assertEquals($this->writer->getSnowflakeConnection()->quote($expectedDefinition->getDefault()), $dbDefinition->getDefault());
        } else {
            $this->assertEquals($expectedDefinition->getDefault(), $dbDefinition->getDefault());
        }

        $this->assertEquals($expectedDefinition->isNullable(), $dbDefinition->isNullable());
        $this->assertEquals($expectedDefinition->getSnowflakeBaseType($timestampTypeMapping), $dbDefinition->getType());
    }

    /**
     * @dataProvider writerAllowedTypes
     */
    public function testWriterAllowedTypes($type)
    {
        $definition = new Definition($type);

        $this->assertEquals($type, $definition->getType());
    }

    /**
     * @dataProvider defaultLengthData
     */
    public function testDefaultLength(string $type)
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
                ]
            ]
        ]);

        $columnsInfo = $this->writer->getTableInfo($tableName);
        $this->assertCount(1, $columnsInfo);

        $columnInfo = reset($columnsInfo);
        $this->assertEquals($tableName, $columnInfo['name']);

        $dbDefinition = Definition::createFromSnowflakeMetadata(reset($columnsInfo));

        $this->assertEquals($expectedDefinition->getSnowflakeDefaultLength(), $dbDefinition->getLength());
        $this->assertEquals($expectedDefinition->getDefault(), $dbDefinition->getDefault());
        $this->assertEquals($expectedDefinition->isNullable(), $dbDefinition->isNullable());
        $this->assertEquals($expectedDefinition->getSnowflakeBaseType($timestampTypeMapping), $dbDefinition->getType());
    }

}
