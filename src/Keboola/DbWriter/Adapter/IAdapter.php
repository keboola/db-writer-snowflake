<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Adapter;

interface IAdapter
{
    public const SLICED_FILES_CHUNK_SIZE = 1000;

    public function generateCreateStageCommand(string $stageName): string;

    public function generateCopyCommands(string $tableName, string $stageName, array $columns): iterable;
}
