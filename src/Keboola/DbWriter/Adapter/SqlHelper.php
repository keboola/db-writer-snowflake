<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Adapter;

use Keboola\DbWriter\Writer\Snowflake;

class SqlHelper
{
    public static function getQuotedColumnsNames(array $columns): array
    {
        return array_map(function ($column) {
            return Snowflake::quoteIdentifier($column['dbName']);
        }, $columns);
    }

    public static function getColumnsTransformation(array $columns): array
    {
        return array_map(
            function ($column, $index) {
                if (!empty($column['nullable'])) {
                    return sprintf("IFF(t.$%d = '', null, t.$%d)", $index + 1, $index + 1);
                }
                return sprintf('t.$%d', $index + 1);
            },
            $columns,
            array_keys($columns)
        );
    }
}
