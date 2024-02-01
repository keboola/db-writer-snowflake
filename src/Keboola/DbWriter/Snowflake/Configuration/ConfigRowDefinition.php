<?php

namespace Keboola\DbWriter\Snowflake\Configuration;

use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Builder\TreeBuilder;
use Symfony\Component\Config\Definition\ConfigurationInterface;

class ConfigRowDefinition implements ConfigurationInterface
{
    public function getConfigTreeBuilder(): TreeBuilder
    {
        $treeBuilder = new TreeBuilder();
        $rootNode = $treeBuilder->root('parameters');

        $rootNode
            ->children()
                ->scalarNode('data_dir')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('writer_class')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->arrayNode('db')
                    ->ignoreExtraKeys(true)
                    ->children()
                        ->scalarNode('runId')->end()
                        ->scalarNode('driver')->end()
                        ->scalarNode('host')
                            ->isRequired()
                            ->cannotBeEmpty()
                        ->end()
                        ->scalarNode('port')->end()
                        ->scalarNode('warehouse')->end()
                        ->scalarNode('database')
                            ->isRequired()
                            ->cannotBeEmpty()
                        ->end()
                        ->scalarNode('schema')
                            ->isRequired()
                            ->cannotBeEmpty()
                        ->end()
                        ->scalarNode('user')
                            ->isRequired()
                            ->cannotBeEmpty()
                        ->end()
                        ->scalarNode('password')->end()
                        ->scalarNode('#password')->end()
                        ->append($this->addSshNode())
                    ->end()
                ->end()
                ->scalarNode('tableId')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->scalarNode('dbName')
                    ->isRequired()
                    ->cannotBeEmpty()
                ->end()
                ->booleanNode('incremental')
                    ->defaultValue(false)
                ->end()
                ->booleanNode('export')
                    ->defaultValue(true)
                ->end()
                ->arrayNode('primaryKey')
                    ->prototype('scalar')
                    ->end()
                ->end()
                ->arrayNode('items')
                    ->prototype('array')
                        ->children()
                            ->scalarNode('name')
                                ->isRequired()
                                ->cannotBeEmpty()
                            ->end()
                            ->scalarNode('dbName')
                                ->isRequired()
                                ->cannotBeEmpty()
                            ->end()
                            ->scalarNode('type')
                                ->isRequired()
                                ->cannotBeEmpty()
                            ->end()
                            ->scalarNode('size')
                            ->end()
                            ->scalarNode('nullable')
                            ->end()
                            ->scalarNode('default')
                            ->end()
                        ->end()
                    ->end()
                ->end()
            ->end()
        ;

        return $treeBuilder;
    }

    public function addSshNode(): ArrayNodeDefinition
    {
        $builder = new TreeBuilder('ssh');

        /** @var ArrayNodeDefinition $node */
        $node = $builder->getRootNode();
        $node
            ->children()
                ->booleanNode('enabled')->end()
                ->arrayNode('keys')
                    ->children()
                        ->scalarNode('private')->end()
                        ->scalarNode('#private')->end()
                        ->scalarNode('public')->end()
                    ->end()
                ->end()
                ->scalarNode('sshHost')->end()
                ->scalarNode('sshPort')
                    ->defaultValue('22')
                ->end()
                ->scalarNode('remoteHost')
                ->end()
                ->scalarNode('remotePort')
                ->end()
                ->scalarNode('localPort')
                    ->defaultValue('33006')
                ->end()
                ->scalarNode('user')->end()
            ->end()
        ;

        return $node;
    }
}
