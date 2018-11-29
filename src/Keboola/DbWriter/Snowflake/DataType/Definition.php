<?php

namespace Keboola\DbWriter\Snowflake\DataType;

use Keboola\Datatype\Definition\Snowflake;

class Definition extends Snowflake
{
    public const TIMESTAMP_TYPE_MAPPING_LTZ = 'TIMESTAMP_LTZ';
    public const TIMESTAMP_TYPE_MAPPING_NTZ = 'TIMESTAMP_NTZ';

    public static function createFromTableMapping(array $column): Definition
    {
        $requiredAttributes = [
            'type',
        ];

        $missingOptions = array_diff($requiredAttributes, array_keys($column));
        if (!empty($missingOptions)) {
            throw new \InvalidArgumentException('Missing column defintion: ' . implode(', ', $missingOptions));
        }

        return new Definition(
            $column['type'],
            [
                "nullable" => isset($column['nullable']) && $column['nullable'] ? true : false,
                "length" => isset($column['size']) ? $column['size'] : null,
                "default" => isset($column['default']) ? $column['default'] : null,
            ]
        );
    }

    public static function stripDefaultValueQuoting(string $text): string
    {
        return (string) preg_replace(
            '/[\']{2}/',
            '\'',
            (string) preg_replace(
                '/(^[\']|[\']$)/',
                '',
                $text
            )
        );
    }

    public static function createFromSnowflakeMetadata(array $meta): Definition
    {
        $requiredMetadata = [
            'kind',
            'type',
            'null?',
            'default',
        ];

        $missingOptions = array_diff($requiredMetadata, array_keys($meta));
        if (!empty($missingOptions)) {
            throw new \InvalidArgumentException('Missing medata: ' . implode(', ', $missingOptions));
        }

        if ($meta['kind'] !== 'COLUMN') {
            throw new \InvalidArgumentException('Metadata does not contains column definition');
        }

        $type = $meta['type'];
        $length = null;

        $matches = [];
        if (preg_match('/^(\w+)\(([0-9\,]+)\)$/ui', $meta['type'], $matches)) {
            $type = $matches[1];
            $length = $matches[2];
        }

        return new Definition(
            $type,
            [
                "nullable" => $meta['null?'] === 'Y' ? true : false,
                "length" => $length,
                "default" => isset($meta['default']) ? self::stripDefaultValueQuoting((string) $meta['default']) : null,
            ]
        );
    }

    public function getSnowflakeDefaultLength()
    {
        switch (strtoupper($this->type)) {
            case "INT":
            case "INTEGER":
            case "BIGINT":
            case "SMALLINT":
            case "TINYINT":
            case "BYTEINT":
            case "NUMBER":
            case "DECIMAL":
            case "NUMERIC":
                $length = "38,0";
                break;
            case "VARCHAR":
            case "STRING":
            case "TEXT":
                $length = "16777216";
                break;
            case "CHAR":
            case "CHARACTER":
                $length = "1";
                break;
            case "TIME":
            case "DATETIME":
            case "TIMESTAMP":
            case "TIMESTAMP_NTZ":
            case "TIMESTAMP_LTZ":
            case "TIMESTAMP_TZ":
                $length = "9";
                break;
            case "BINARY":
            case "VARBINARY":
                $length = "8388608";
                break;
            default:
                $length = null;
                break;
        }
        return $length;
    }

    /**
     * @param string $timestampTypeMapping
     * @return string
     */
    public function getSnowflakeBaseType(string $timestampTypeMapping)
    {
        if ($timestampTypeMapping !== self::TIMESTAMP_TYPE_MAPPING_LTZ && $timestampTypeMapping !== self::TIMESTAMP_TYPE_MAPPING_NTZ) {
            throw new \InvalidArgumentException(sprintf(
                'Invalid Snowflake timestamp type mapping provided: "%s"',
                $timestampTypeMapping
            ));
        }

        switch (strtoupper($this->type)) {
            case "INT":
            case "INTEGER":
            case "BIGINT":
            case "SMALLINT":
            case "TINYINT":
            case "BYTEINT":
            case "NUMBER":
            case "DECIMAL":
            case "NUMERIC":
                $basetype = "NUMBER";
                break;
            case "FLOAT":
            case "FLOAT4":
            case "FLOAT8":
            case "DOUBLE":
            case "DOUBLE PRECISION":
            case "REAL":
                $basetype = "FLOAT";
                break;
            case "BOOLEAN":
                $basetype = "BOOLEAN";
                break;
            case "DATE":
                $basetype = "DATE";
                break;
            case "TIME":
                $basetype = "TIME";
                break;
            case "DATETIME":
                $basetype = "TIMESTAMP_NTZ";
                break;
            case "TIMESTAMP":
                $basetype = $timestampTypeMapping;
                break;
            case "TIMESTAMP_NTZ":
                $basetype = "TIMESTAMP_NTZ";
                break;
            case "TIMESTAMP_LTZ":
                $basetype = "TIMESTAMP_LTZ";
                break;
            case "TIMESTAMP_TZ":
                $basetype = "TIMESTAMP_TZ";
                break;
            case "VARIANT":
                $basetype = "VARIANT";
                break;
            case "ARRAY":
                $basetype = "ARRAY";
                break;
            case "OBJECT":
                $basetype = "OBJECT";
                break;
            case "BINARY":
            case "VARBINARY":
                $basetype = "BINARY";
                break;
            default:
                $basetype = "VARCHAR";
                break;
        }
        return $basetype;
    }

    /**
     * @return string
     */
    public function getSQLDefinition()
    {
        $definition = parent::getSQLDefinition();

        if ($this->getDefault() !== null) {
            if ($this->getBasetype() === 'STRING') {
                $definition .= sprintf(" DEFAULT '%s'", addslashes($this->getDefault()));
            } else {
                $definition .= sprintf(" DEFAULT %s", addslashes($this->getDefault()));
            }
        }

        return $definition;
    }
}
