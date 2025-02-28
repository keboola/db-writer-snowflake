<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer\Strategy;

use Keboola\DbWriter\Writer\QuoteTrait;
use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;

class SqlHelper
{
    use QuoteTrait;
    /**
     * @param ItemConfig[] $items
     */
    public static function getQuotedColumnsNames(array $items): array
    {
        return array_map(fn($column) => (new SqlHelper)->quoteIdentifier($column->getDbName()), $items);
    }

    /**
     * @param ItemConfig[] $items
     */
    public static function getColumnsTransformation(array $items): array
    {
        return array_map(
            function (ItemConfig $item, int $index) {
                if ($item->getNullable()) {
                    return sprintf("IFF(t.$%d = '', null, t.$%d)", $index + 1, $index + 1);
                }
                return sprintf('t.$%d', $index + 1);
            },
            $items,
            array_keys($items),
        );
    }
}
