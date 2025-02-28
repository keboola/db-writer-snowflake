<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Writer;

use Keboola\DbWriterAdapter\ODBC\OdbcConnection;

class SnowflakeConnection extends OdbcConnection
{
    use QuoteTrait;
}
