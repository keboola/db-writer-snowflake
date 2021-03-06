<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake;

use Keboola\DbWriter\Adapter\IAdapter;
use Keboola\DbWriter\Adapter\NullAdapter;
use Keboola\DbWriter\Writer\Snowflake;
use Monolog\Logger;
use Keboola\DbWriter\WriterFactory;

class SnowflakeWriterFactory extends WriterFactory
{
    private array $parameters;

    public function __construct(array $parameters)
    {
        $this->parameters = $parameters;
    }

    public function create(Logger $logger, ?IAdapter $adapter = null): Snowflake
    {
        if (!$adapter) {
            $adapter = new NullAdapter();
        }
        return new Snowflake($this->parameters['db'], $logger, $adapter);
    }
}
