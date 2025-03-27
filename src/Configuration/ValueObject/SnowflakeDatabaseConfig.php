<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Configuration\ValueObject;

use Keboola\DbWriterConfig\Configuration\ValueObject\DatabaseConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\SshConfig;
use Keboola\DbWriterConfig\Configuration\ValueObject\SslConfig;
use Keboola\DbWriterConfig\Exception\PropertyNotSetException;

readonly class SnowflakeDatabaseConfig extends DatabaseConfig
{
    public function __construct(
        ?string $host,
        ?string $port,
        string $database,
        private ?string $warehouse,
        private ?string $runId,
        string $user,
        ?string $password,
        ?string $schema,
        ?SshConfig $sshConfig,
        ?SslConfig $sslConfig,
    ) {
        parent::__construct($host, $port, $database, $user, $password, $schema, $sshConfig, $sslConfig);
    }

    public static function fromArray(array $config): self
    {
        $sshEnabled = $config['ssh']['enabled'] ?? false;
        $sslEnabled = $config['ssl']['enabled'] ?? false;
        $runId = $config['runId'] ?? getenv('KBC_RUNID');

        return new self(
            $config['host'],
            $config['port'] ?? null,
            $config['database'],
            $config['warehouse'] ?? null,
            $runId ?: null,
            $config['user'],
            $config['#password'],
            $config['schema'],
            $sshEnabled ? SshConfig::fromArray($config['ssh']) : null,
            $sslEnabled ? SslConfig::fromArray($config['ssl']) : null,
        );
    }

    public function hasWarehouse(): bool
    {
        return $this->warehouse !== null;
    }

    public function getWarehouse(): string
    {
        if ($this->warehouse === null) {
            throw new PropertyNotSetException('Property "warehouse" is not set.');
        }
        return $this->warehouse;
    }

    public function hasRunId(): bool
    {
        return $this->runId !== null;
    }

    public function getRunId(): string
    {
        if ($this->runId === null) {
            throw new PropertyNotSetException('Property "runId" is not set.');
        }
        return $this->runId;
    }
}
