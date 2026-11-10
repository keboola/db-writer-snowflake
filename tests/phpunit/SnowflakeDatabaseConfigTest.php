<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\DbWriter\Configuration\ValueObject\SnowflakeDatabaseConfig;
use PHPUnit\Framework\TestCase;

class SnowflakeDatabaseConfigTest extends TestCase
{
    /**
     * SshTunnel::createSshTunnel() rebuilds the config via toArray() -> fromArray();
     * the Snowflake-specific properties must survive that round-trip.
     */
    public function testToArrayRoundTripKeepsSnowflakeProperties(): void
    {
        $config = SnowflakeDatabaseConfig::fromArray([
            'host' => 'example.snowflakecomputing.com',
            'port' => '443',
            'database' => 'db',
            'warehouse' => 'wh',
            'roleName' => 'role',
            'runId' => '123',
            'user' => 'user',
            '#privateKey' => 'private-key',
            'schema' => 'schema',
        ]);

        $restored = SnowflakeDatabaseConfig::fromArray($config->toArray());

        self::assertSame('private-key', $restored->getPrivateKey());
        self::assertSame('wh', $restored->getWarehouse());
        self::assertSame('role', $restored->getRoleName());
        self::assertSame('123', $restored->getRunId());
        self::assertSame('example.snowflakecomputing.com', $restored->getHost());
        self::assertSame('443', $restored->getPort());
        self::assertSame('db', $restored->getDatabase());
        self::assertSame('user', $restored->getUser());
        self::assertSame('schema', $restored->getSchema());
    }
}
