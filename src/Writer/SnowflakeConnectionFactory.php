<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriter\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Psr\Log\LoggerInterface;

class SnowflakeConnectionFactory
{
    use QuoteTrait;

    private const SNOWFLAKE_APPLICATION = 'Keboola_Connection';

    public function create(SnowflakeDatabaseConfig $databaseConfig, LoggerInterface $logger): SnowflakeConnection
    {
        $connection = new SnowflakeConnection(
            $logger,
            $this->generateDsn($databaseConfig),
            $databaseConfig->getUser(),
            self::escapePassword($databaseConfig->getPassword()),
            function ($connection) use ($databaseConfig) {
                if ($databaseConfig->hasRunId()) {
                    $queryTag = [
                        'runId' => $databaseConfig->getRunId(),
                    ];
                    odbc_exec(
                        $connection,
                        sprintf(
                            'ALTER SESSION SET QUERY_TAG=\'%s\';',
                            json_encode($queryTag),
                        ),
                    );
                }
            },
        );

        return $connection;
    }

    public static function escapePassword(string $password): string
    {
        if (is_int(strpos($password, ';'))) {
            return '{' . str_replace('}', '}}', $password) . '}';
        }

        return $password;
    }

    private function generateDsn(SnowflakeDatabaseConfig $databaseConfig): string
    {
        $dsn = 'Driver=SnowflakeDSIIDriver;Server=' . $databaseConfig->getHost();
        $dsn .= ';Port=' . ($databaseConfig->hasPort() ? $databaseConfig->getPort() : 443);
        $dsn .= ';Tracing=0';
        $dsn .= ';Login_timeout=30';
        $dsn .= ';Database=' . $this->quoteIdentifier($databaseConfig->getDatabase());
        $dsn .= ';Schema=' . $this->quoteIdentifier($databaseConfig->getSchema());

        if ($databaseConfig->hasWarehouse()) {
            $dsn .= ';Warehouse=' . $databaseConfig->getWarehouse();
        }
        $dsn .= ';CLIENT_SESSION_KEEP_ALIVE=TRUE';
        $dsn .= ';application=' . $this->quoteIdentifier(self::SNOWFLAKE_APPLICATION);

        return $dsn;
    }
}
