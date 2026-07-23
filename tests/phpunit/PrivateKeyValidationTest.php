<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\DbWriter\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Writer\Snowflake;
use Keboola\SnowflakeDbAdapter\Exception\PrivateKeyIsNotValid;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;

class PrivateKeyValidationTest extends TestCase
{
    /**
     * An invalid key-pair private key must surface as a UserException (user error),
     * not as the uncaught PrivateKeyIsNotValid (internal error). The invalid key is
     * rejected while building the DSN, before any connection to Snowflake is opened,
     * so this does not require a live database.
     */
    public function testInvalidPrivateKeyIsReportedAsUserException(): void
    {
        $databaseConfig = new SnowflakeDatabaseConfig(
            'localhost',
            null,
            'testdb',
            'testwh',
            null,
            null,
            'testuser',
            '',
            'this-is-not-a-valid-private-key',
            'testschema',
            null,
            null,
        );

        try {
            new Snowflake($databaseConfig, new NullLogger());
            $this->fail('Creating the writer with an invalid private key should throw a UserException.');
        } catch (UserException $e) {
            $this->assertStringContainsString('private key is not valid', $e->getMessage());
            $this->assertInstanceOf(PrivateKeyIsNotValid::class, $e->getPrevious());
        }
    }
}
