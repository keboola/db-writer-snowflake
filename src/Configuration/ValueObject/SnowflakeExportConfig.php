<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Configuration\ValueObject;

use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\ExportConfig;
use Keboola\DbWriterConfig\Exception\UserException;

readonly class SnowflakeExportConfig extends ExportConfig
{
    public static function fromArray(array $config, array $inputMapping, ?DatabaseConfig $databaseConfig = null): self
    {
        $filteredInputMapping = array_filter($inputMapping, fn($v) => $v['source'] === $config['tableId']);
        if (count($filteredInputMapping) === 0) {
            throw new UserException(
                sprintf('Table "%s" in storage input mapping cannot be found.', $config['tableId']),
            );
        }
        $tableFilePath = sprintf(
            '%s/in/tables/%s',
            $config['data_dir'],
            current($filteredInputMapping)['destination'],
        );

        return new self(
            $config['data_dir'],
            $config['writer_class'],
            $databaseConfig ?? DatabaseConfig::fromArray($config['db']),
            $config['tableId'],
            $config['dbName'],
            $config['incremental'] ?? false,
            $config['export'] ?? true,
            !empty($config['primaryKey']) ? $config['primaryKey'] : null,
            array_map(fn($v) => SnowflakeItemConfig::fromArray($v), $config['items']),
            $tableFilePath,
        );
    }
}
