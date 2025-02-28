<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake\FunctionalTests;

use PHPUnit\Framework\Assert;

return function (DatadirTest $test): void {
    $sql = <<<SQL
SELECT * 
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND CONSTRAINT_TYPE = 'FOREIGN KEY'
SQL;

    $databaseConfig = $test->getDatabaseConfig();
    $foreignKeys = $test->connection->fetchAll(sprintf(
        $sql,
        $databaseConfig->getSchema(),
        'simple',
    ));

    Assert::assertCount(1, $foreignKeys);
    Assert::assertEquals('FK_SPECIAL_COL1', $foreignKeys[0]['CONSTRAINT_NAME']);
};
