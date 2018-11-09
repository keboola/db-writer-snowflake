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

    public function testDefinitionFactory()
    {
        $connection = $this->writer->getSnowflakeConnection();

        $tableName = 'testDefinitionFactory';
        $expectedDefinition = new Definition(
            'VARCHAR',
            [
                'length' => 50,
                'nullable' => false,
//                'default' => 'mydefault',
            ]
        );

        $this->writer->drop($tableName);

        $sql = sprintf(
            'CREATE TABLE %s (%s VARCHAR(50) NOT NULL DEFAULT \'mydefault\');',
            $connection->quoteIdentifier($tableName),
            $connection->quoteIdentifier("test")
        );

        $connection->query($sql);

        $columnsInfo = $this->writer->getTableInfo($tableName);
        $this->assertCount(1, $columnsInfo);

        $columnInfo = reset($columnsInfo);
        $this->assertEquals('test', $columnInfo['name']);

        $dbDefinition = Definition::createFromSnowflakeMetadata(reset($columnsInfo));

        $this->assertEquals($expectedDefinition->getLength(), $dbDefinition->getLength());
        $this->assertEquals($expectedDefinition->getDefault(), $dbDefinition->getDefault());
        $this->assertEquals($expectedDefinition->isNullable(), $dbDefinition->isNullable());
        $this->assertEquals($expectedDefinition->getSnowflakeBaseType(), $dbDefinition->getType());
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
        $connection = $this->writer->getSnowflakeConnection();

        $tableName = 'testDefaultLength';
        $expectedDefinition = new Definition($type, ['nullable' => false]);

        $this->writer->drop($tableName);

        $sql = sprintf(
            'CREATE TABLE %s (%s %s NOT NULL);',
            $connection->quoteIdentifier($tableName),
            $connection->quoteIdentifier($tableName),
            $type
        );

        $connection->query($sql);

        $columnsInfo = $this->writer->getTableInfo($tableName);
        $this->assertCount(1, $columnsInfo);

        $columnInfo = reset($columnsInfo);
        $this->assertEquals($tableName, $columnInfo['name']);

        $dbDefinition = Definition::createFromSnowflakeMetadata(reset($columnsInfo));

        $this->assertEquals($expectedDefinition->getSnowflakeDefaultLength(), $dbDefinition->getLength());
        $this->assertEquals($expectedDefinition->getDefault(), $dbDefinition->getDefault());
        $this->assertEquals($expectedDefinition->isNullable(), $dbDefinition->isNullable());
        $this->assertEquals($expectedDefinition->getSnowflakeBaseType(), $dbDefinition->getType());
    }

}
