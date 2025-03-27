<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake\Tests;

use Generator;
use Keboola\DbWriter\Writer\SnowflakeConnectionFactory;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class SnowflakeConnectionTest extends TestCase
{

    /**
     * @dataProvider passwordsDataProvider
     */
    public function testPasswords(string $password, string $expectedPassword): void
    {
        Assert::assertEquals($expectedPassword, SnowflakeConnectionFactory::escapePassword($password));
    }

    public function passwordsDataProvider(): Generator
    {
        yield 'simple-password' => [
            'AbcdEfgh123456',
            'AbcdEfgh123456',
        ];

        yield 'password-with-semicolon' => [
            'AbcdEfgh12;3456',
            '{AbcdEfgh12;3456}',
        ];

        yield 'password-with-bracket' => [
            'AbcdEfgh12}3456',
            'AbcdEfgh12}3456',
        ];

        yield 'password-with-semicolon-and-bracket' => [
            'AbcdEf;gh12}3456',
            '{AbcdEf;gh12}}3456}',
        ];

        yield 'password-starts-with-semicolon' => [
            ';AbcdEfgh12345}6',
            '{;AbcdEfgh12345}}6}',
        ];
    }
}
