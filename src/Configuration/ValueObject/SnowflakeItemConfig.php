<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Configuration\ValueObject;

use Keboola\DbWriterConfig\Configuration\ValueObject\ItemConfig;
use Keboola\DbWriterConfig\Exception\PropertyNotSetException;

readonly class SnowflakeItemConfig extends ItemConfig
{
    /**
     * @param $config array{
     *     name: string,
     *     dbName: string,
     *     type: string,
     *     size?: string,
     *     nullable?: bool,
     *     default?: string,
     *     foreignKeyTable?: string,
     *     foreignKeyColumn?: string,
     * }
     */
    public static function fromArray(array $config): self
    {
        return new self(
            $config['name'],
            $config['dbName'],
            $config['type'],
            $config['size'] ? (string) $config['size'] : null,
            $config['nullable'] ?? null,
            $config['default'] ?? null,
            $config['foreignKeyTable'] ?? null,
            $config['foreignKeyColumn'] ?? null,
        );
    }

    public function __construct(
        string $name,
        string $dbName,
        string $type,
        ?string $size,
        ?bool $nullable,
        ?string $default,
        private ?string $foreignKeyTable,
        private ?string $foreignKeyColumn,
    ) {
        parent::__construct($name, $dbName, $type, $size, $nullable, $default);
    }

    public function hasForeignKey(): bool
    {
        return !empty($this->foreignKeyTable) && !empty($this->foreignKeyColumn);
    }

    public function getForeignKeyTable(): string
    {
        if (!$this->foreignKeyTable) {
            throw new PropertyNotSetException('Property "foreignKeyTable" is not set.');
        }
        return $this->foreignKeyTable;
    }

    public function getForeignKeyColumn(): string
    {
        if (!$this->foreignKeyColumn) {
            throw new PropertyNotSetException('Property "foreignKeyColumn" is not set.');
        }
        return $this->foreignKeyColumn;
    }
}
