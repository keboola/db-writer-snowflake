<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Adapter;

interface IAdapter
{
    public function generateCreateStageCommand(string $stageName): string;

    public function generateCopyCommand(string $tableName, string $stageName, array $columns): string;
}
