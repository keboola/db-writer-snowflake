<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake\Tests;

use Generator;
use Keboola\DbWriter\Configuration\NodeDefinition\SnowflakeDbNode;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\Processor;

class SnowflakeDbNodeTest extends TestCase
{
    /**
     * Sync actions use the db node without ignoreExtraKeys(), so a leftover password
     * in a stored config must be dropped instead of failing as an unrecognized option.
     *
     * @dataProvider legacyPasswordProvider
     */
    public function testLegacyPasswordIsDropped(array $password): void
    {
        $treeBuilder = new TreeBuilder('root');
        /** @var ArrayNodeDefinition $rootNode */
        $rootNode = $treeBuilder->getRootNode();
        $rootNode->children()->append(new SnowflakeDbNode());

        $processed = (new Processor())->process($treeBuilder->buildTree(), [[
            'db' => [
                'host' => 'host',
                'database' => 'db',
                'schema' => 'schema',
                'user' => 'user',
                '#privateKey' => 'private-key',
            ] + $password,
        ]]);

        self::assertArrayNotHasKey('#password', $processed['db']);
        self::assertArrayNotHasKey('password', $processed['db']);
        self::assertSame('private-key', $processed['db']['#privateKey']);
    }

    public function legacyPasswordProvider(): Generator
    {
        yield 'encrypted password' => [['#password' => 'secret']];
        yield 'empty encrypted password' => [['#password' => '']];
        yield 'plain password' => [['password' => 'secret']];
    }
}
