<?php

namespace Keboola\DbWriter\Snowflake\DataType;

use Keboola\Datatype\Definition\Snowflake;

class Definition extends Snowflake
{
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
//                "default" => $meta['default'], //@FIXME default value quoting for string type
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
     * @return string
     */
    public function getSnowflakeBaseType()
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
            //@FIXME https://docs.snowflake.net/manuals/sql-reference/parameters.html#label-timestamp-type-mapping
            case "TIMESTAMP":
                $basetype = "TIMESTAMP_LTZ";
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
}
