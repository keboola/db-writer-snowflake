<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Snowflake\PrepareTestsData;

require_once __DIR__ . '/../../vendor/autoload.php';

use Keboola\StorageApi\Client;
use Symfony\Component\Finder\Finder;

$storageLoader = new StagingStorageLoader(
    __DIR__ . '/tables',
    new Client([
        'url' => getenv('KBC_URL'),
        'token' => getenv('STORAGE_API_TOKEN'),
    ]),
);
$finder = new Finder();
$files = $finder
    ->files()
    ->in(__DIR__ . '/tables')
    ->name('*.csv');

$dataFilesMetadata = [];
foreach ($files as $file) {
    $dataFilesMetadata[$file->getFilename()] = $storageLoader->upload($file->getFilenameWithoutExtension());
}

file_put_contents(
    __DIR__ . '/manifestData.json',
    json_encode($dataFilesMetadata, JSON_PRETTY_PRINT),
);
