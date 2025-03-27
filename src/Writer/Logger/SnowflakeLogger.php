<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer\Logger;

use Keboola\Component\Logger;

class SnowflakeLogger extends Logger
{
    public function debug($message, array $context = []): void
    {
        $secretMessage = $this->hideCredentials((string) $message);
        parent::debug($secretMessage ?? $message, $context);
    }

    public function info($message, array $context = []): void
    {
        $secretMessage = $this->hideCredentials((string) $message);
        parent::info($secretMessage ?? $message, $context);
    }

    private function hideCredentials(string $query): ?string
    {
        return preg_replace(
            '/(AZURE_[A-Z_]*\\s=\\s.|AWS_[A-Z_]*\\s=\\s.)[0-9A-Za-z\\/\\+=\\-&:%]*/',
            '${1}...\'',
            $query,
        );
    }
}
