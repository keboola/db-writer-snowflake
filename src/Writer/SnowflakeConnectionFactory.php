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
            'privateKey' => $databaseConfig->getPrivateKey(),
            'database' => $databaseConfig->getDatabase(),
            'schema' => $databaseConfig->getSchema(),
            'warehouse' => $databaseConfig->hasWarehouse() ? $databaseConfig->getWarehouse() : null,
            'clientSessionKeepAlive' => true,
            'application' => self::SNOWFLAKE_APPLICATION,
            'loginTimeout' => 30,
        ];

        if (!empty($databaseConfig->getRoleName())) {
            $options['roleName'] = $databaseConfig->getRoleName();
        }

        return new SnowflakeConnection(
            $logger,
            DSNBuilder::build($options),
            $databaseConfig->getUser(),
            '',
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
}
