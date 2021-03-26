<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Adapter;

use Keboola\DbWriter\Exception\ApplicationException;

class NullAdapter implements IAdapter
{
    public function generateCreateStageCommand(string $stageName): string
    {
        throw new ApplicationException('Method "generateCreateStageCommand" not implemented');
    }

    public function generateCopyCommands(string $tableName, string $stageName, array $columns): iterable
    {
        throw new ApplicationException('Method "generateCopyCommand" not implemented');
    }
}
