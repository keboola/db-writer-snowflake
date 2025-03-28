<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriter\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\SnowflakeDbAdapter\Builder\DSNBuilder;
use Psr\Log\LoggerInterface;

class SnowflakeConnectionFactory
{
    use QuoteTrait;

    private const SNOWFLAKE_APPLICATION = 'Keboola_Connection';

    public function create(SnowflakeDatabaseConfig $databaseConfig, LoggerInterface $logger): SnowflakeConnection
    {
        /** @var string[] $options */
        $options = [
            'host' => $databaseConfig->getHost(),
            'port' => $databaseConfig->hasPort() ? $databaseConfig->getPort() : 443,
            'user' => $databaseConfig->getUser(),
            'password' => $databaseConfig->getPassword(),
            'keyPair' => $databaseConfig->getKeyPair(),
            'database' => $databaseConfig->getDatabase(),
            'schema' => $databaseConfig->getSchema(),
            'warehouse' => $databaseConfig->hasWarehouse() ? $databaseConfig->getWarehouse() : null,
            'clientSessionKeepAlive' => true,
            'application' => self::SNOWFLAKE_APPLICATION,
            'loginTimeout' => 30,
        ];

        return new SnowflakeConnection(
            $logger,
            DSNBuilder::build($options),
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
    }

    public static function escapePassword(string $password): string
    {
        if (is_int(strpos($password, ';'))) {
            return '{' . str_replace('}', '}}', $password) . '}';
        }

        return $password;
    }
}
