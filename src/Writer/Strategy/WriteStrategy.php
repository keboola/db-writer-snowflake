<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer\Strategy;

interface WriteStrategy
{
    public const FILE_STORAGE_S3 = 's3';
    public const FILE_STORAGE_ABS = 'abs';
    public const SLICED_FILES_CHUNK_SIZE = 1000;

    public function generateCreateStageCommand(string $stageName): string;
    public function generateCopyCommands(string $tableName, string $stageName, array $items): iterable;
}
