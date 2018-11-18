<?php
declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\DbWriter\Test\BaseTest as CommonBaseTest;
use Keboola\DbWriter\Writer\Snowflake;

abstract class BaseTest extends CommonBaseTest
{
    protected function getConfig(?string $dataDir = null): array
    {
        $config = parent::getConfig($dataDir);
        $config['parameters']['writer_class'] = Snowflake::WRITER;
        $config['parameters']['db']['schema'] = $this->getEnv('DB_SCHEMA');
        $config['parameters']['db']['warehouse'] = $this->getEnv('DB_WAREHOUSE');
        $config['parameters']['db']['password'] = $config['parameters']['db']['#password'];

        return $config;
    }
}
