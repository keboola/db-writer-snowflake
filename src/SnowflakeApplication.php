<?php

declare(strict_types=1);

namespace Keboola\DbWriter;

use Keboola\Component\Config\BaseConfig;
use Keboola\Component\UserException;
use Keboola\DbWriter\Configuration\NodeDefinition\SnowflakeDbNode;
use Keboola\DbWriter\Configuration\SnowflakeTableNodesDecorator;
use Keboola\DbWriter\Configuration\ValueObject\SnowflakeDatabaseConfig;
use Keboola\DbWriter\Configuration\ValueObject\SnowflakeExportConfig;
use Keboola\DbWriter\Writer\Snowflake;
use Keboola\DbWriterConfig\Configuration\ConfigDefinition;
use Keboola\DbWriterConfig\Configuration\ConfigRowDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class SnowflakeApplication extends Application
{
    protected string $writerName = 'Snowflake';

    protected function run(): void
    {
        $parameters = $this->getConfig()->getParameters();
        $writerFactory = new WriterFactory($this->getConfig());
        /** @var Snowflake $writer */
        $writer = $writerFactory->create($this->getLogger(), $this->createDatabaseConfig($parameters['db']));

        if (!$this->isRowConfiguration($parameters)) {
            $filteredTables = array_filter($parameters['tables'], fn($table) => $table['export']);
            unset($parameters['tables']);
            foreach ($filteredTables as $k => $filteredTable) {
                $filteredTable = $this->validateTableItems($filteredTable);
                $filteredTable = array_merge($parameters, $filteredTable);
                $filteredTables[$k] = $filteredTable;
                $writer->write($this->createExportConfig($filteredTable));
            }
            foreach ($filteredTables as $filteredTable) {
                $writer->createForeignKeys($this->createExportConfig($filteredTable));
            }
        } else {
            $parameters = $this->validateTableItems($parameters);
            $writer->write($this->createExportConfig($parameters));
        }
    }

    protected function loadConfig(): void
    {
        $configClass = $this->getConfigClass();
        $configDefinitionClass = $this->getConfigDefinitionClass();

        if (in_array($configDefinitionClass, [ConfigRowDefinition::class, ConfigDefinition::class])) {
            $definition = new $configDefinitionClass(
                dbNode: (new SnowflakeDbNode())->ignoreExtraKeys(),
                tableNodesDecorator: new SnowflakeTableNodesDecorator(),
            );
        } else {
            $definition = new $configDefinitionClass(dbNode: new SnowflakeDbNode());
        }

        try {
            /** @var BaseConfig $config */
            $config = new $configClass(
                $this->getRawConfig(),
                $definition,
            );
            $this->config = $config;
        } catch (InvalidConfigurationException $e) {
            throw new UserException($e->getMessage(), 0, $e);
        }
    }

    protected function createDatabaseConfig(array $dbParams): SnowflakeDatabaseConfig
    {
        return SnowflakeDatabaseConfig::fromArray($dbParams);
    }

    protected function createExportConfig(array $table): SnowflakeExportConfig
    {
        return SnowflakeExportConfig::fromArray(
            $table,
            $this->getConfig()->getInputTables(),
            $this->createDatabaseConfig($table['db']),
        );
    }
}
