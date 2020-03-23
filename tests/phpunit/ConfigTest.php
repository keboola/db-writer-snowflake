<?php

namespace Keboola\DbWriter\Snowflake\Tests;

use Keboola\DbWriter\Configuration\Validator;
use Keboola\DbWriter\Snowflake\Configuration\ConfigRowDefinition;
use Webmozart\Assert\Assert;

class ConfigTest extends BaseTest
{
    public function testDefaultValues(): void
    {
        $input = [
            'parameters' => [
                'data_dir' => '...',
                'writer_class' => '...',
                'dbName' => 'stock',
                'items' => [
                    [
                        'dbName' => 'product_id',
                        'default' => '',
                        'name' => 'product_id',
                        'nullable' => false,
                        'size' => '255',
                        'type' => 'string',
                    ],
                ],
                'primaryKey' => ['product_id'],
                'tableId' => 'in.c-keboola-ex-db-snowflake-554446708.stock',
            ],
        ];

        $validate = Validator::getValidator(new ConfigRowDefinition);
        $config = $validate($input['parameters']);
        $this->assertSame(false, $config['incremental']);
        $this->assertSame(true, $config['export']);
    }
}
