<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Configuration;

use Keboola\DbWriterConfig\Configuration\NodeDefinition\TableNodesDecorator;
use Symfony\Component\Config\Definition\Builder\NodeBuilder;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class SnowflakeTableNodesDecorator extends TableNodesDecorator
{
    protected function addItemsNode(NodeBuilder $nodeBuilder): void
    {
        $nodeBuilder
            ->arrayNode('items')
                ->validate()->always(function ($v) {
                    $validItem = false;
                    foreach ($v as $item) {
                        if ($item['type'] !== 'ignore') {
                            $validItem = true;
                            break;
                        }
                    }
                    if (!$validItem) {
                        throw new InvalidConfigurationException(
                            'At least one item must be defined and cannot be ignored.',
                        );
                    }
                    return $v;
                })->end()
                ->arrayPrototype()
                ->children()
                    ->scalarNode('name')->isRequired()->cannotBeEmpty()->end()
                    ->scalarNode('dbName')->isRequired()->cannotBeEmpty()->end()
                    ->scalarNode('type')->isRequired()->cannotBeEmpty()->end()
                    ->scalarNode('size')->beforeNormalization()->always(fn($v) => (string) $v)->end()->end()
                    ->scalarNode('nullable')->end()
                    ->scalarNode('default')->end()
                    ->scalarNode('foreignKeyTable')->cannotBeEmpty()->end()
                    ->scalarNode('foreignKeyColumn')->cannotBeEmpty()->end()
                ->end()
            ->end();
    }
}
