<?php

namespace Keboola\DbWriter\Snowflake;

use Keboola\DbWriter\Exception\UserException;

class Connection
{
    /**
     * @var resource odbc handle
     */
    private $connection;

    /**
     * The connection constructor accepts the following options:
     * - host (string, required) - hostname
     * - port (int, optional) - port - default 443
     * - user (string, required) - username
     * - password (string, required) - password
     * - warehouse (string) - default warehouse to use
     * - database (string) - default database to use
     * - tracing (int) - the level of detail to be logged in the driver trace files
     * - loginTimeout (int) - Specifies how long to wait for a response when connecting to the Snowflake service before returning a login failure error.
     * - networkTimeout (int) - Specifies how long to wait for a response when interacting with the Snowflake service before returning an error. Zero (0) indicates no network timeout is set.
     * - queryTimeout (int) - Specifies how long to wait for a query to complete before returning an error. Zero (0) indicates to wait indefinitely.
     *
     * @param array $options
     * @throws Exception
     */
    public function __construct(array $options)
    {
        $requiredOptions = [
            'host',
            'user',
            'password',
            'database',
            'schema',
        ];

        $missingOptions = array_diff($requiredOptions, array_keys($options));
        if (!empty($missingOptions)) {
            throw new Exception('Missing options: ' . implode(', ', $missingOptions));
        }

        $port = isset($options['port']) ? (int) $options['port'] : 443;
        $tracing = isset($options['tracing']) ? (int) $options['tracing'] : 0;
        $maxBackoffAttempts = isset($options['maxBackoffAttempts']) ? (int) $options['maxBackoffAttempts'] : 5;
        $loginTimeout = isset($options['loginTimeout']) ? (int) $options['loginTimeout'] : 30;

        $dsn = 'Driver=SnowflakeDSIIDriver;Server=' . $options['host'];
        $dsn .= ';Port=' . $port;
        $dsn .= ';Tracing=' . $tracing;
        $dsn .= ';Login_timeout=' . $loginTimeout;
        $dsn .= ';Database=' . $this->quoteIdentifier($options['database']);
        $dsn .= ';Schema=' . $this->quoteIdentifier($options['schema']);

        if (isset($options['networkTimeout'])) {
            $dsn .= ';Network_timeout=' . (int) $options['networkTimeout'];
        }

        if (isset($options['queryTimeout'])) {
            $dsn .= ';Query_timeout=' . (int) $options['queryTimeout'];
        }

        if (isset($options['warehouse'])) {
            $dsn .= ';Warehouse=' . $this->quoteIdentifier($options['warehouse']);
        }
        $dsn .= ";CLIENT_SESSION_KEEP_ALIVE=TRUE";

        $attemptNumber = 0;
        do {
            if ($attemptNumber > 0) {
                sleep(pow(2, $attemptNumber));
            }
            try {
                $this->connection = odbc_connect($dsn, $options['user'], $options['password']);

                if (isset($options['runId'])) {
                    $queryTag = [
                        'runId' => $options['runId'],
                    ];
                    $this->query("ALTER SESSION SET QUERY_TAG='" . json_encode($queryTag) . "';");
                }
            } catch (\Throwable $e) {
                // try again if it is a failed rest request
                if (stristr($e->getMessage(), 'S1000') !== false) {
                    $attemptNumber++;
                    if ($attemptNumber > $maxBackoffAttempts) {
                        throw new Exception('Initializing Snowflake connection failed: ' . $e->getMessage(), 0, $e);
                    }
                } else {
                    throw new Exception('Initializing Snowflake connection failed: ' . $e->getMessage(), 0, $e);
                }
            }
        } while ($this->connection === null);
    }

    public function quoteIdentifier($value)
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }

    /**
     * Returns information about table:
     *  - name
     *  - bytes
     *  - rows
     * @throws Exception
     */
    public function describeTable(string $schemaName, string $tableName): array
    {
        $tables = $this->fetchAll(sprintf(
            'SHOW TABLES LIKE %s IN SCHEMA %s',
            "'" . addslashes($tableName) . "'",
            $this->quoteIdentifier($schemaName)
        ));

        foreach ($tables as $table) {
            if ($table['name'] === $tableName) {
                return $table;
            }
        }

        throw new Exception("Table $tableName not found in schema $schemaName");
    }

    public function describeTableColumns($schemaName, $tableName)
    {
        return $this->fetchAll(sprintf(
            'SHOW COLUMNS IN %s.%s',
            $this->quoteIdentifier($schemaName),
            $this->quoteIdentifier($tableName)
        ));
    }

    public function getTableColumns($schemaName, $tableName)
    {
        return array_map(function ($column) {
            return $column['column_name'];
        }, $this->describeTableColumns($schemaName, $tableName));
    }

    public function getColumnDataType(string $schema, string $table, string $columnName): \stdClass
    {
        $columns = $this->describeTableColumns($schema, $table);
        $column = array_values(array_filter($columns, function ($v) use ($columnName) {
            if ($v['column_name'] !== $columnName) {
                return false;
            }
            return true;
        }));

        if (count($column) === 0) {
            throw new UserException(sprintf('Column \'%s\' in table \'%s\' not found', $columnName, $table));
        }

        return json_decode($column[0]['data_type']);
    }

    public function getTablePrimaryKey($schemaName, $tableName)
    {
        $cols = $this->fetchAll(sprintf(
            'DESC TABLE %s.%s',
            $this->quoteIdentifier($schemaName),
            $this->quoteIdentifier($tableName)
        ));
        $pkCols = [];
        foreach ($cols as $col) {
            if ($col['primary key'] !== 'Y') {
                continue;
            }
            $pkCols[] = $col['name'];
        }

        return $pkCols;
    }

    public function getTableUniqueKeys(string $schemaName, string $tableName): array
    {
        $cols = $this->fetchAll(sprintf(
            'DESC TABLE %s.%s',
            $this->quoteIdentifier($schemaName),
            $this->quoteIdentifier($tableName)
        ));
        $uniqueCols = [];
        foreach ($cols as $col) {
            if ($col['unique key'] !== 'Y') {
                continue;
            }
            $uniqueCols[] = $col['name'];
        }

        return $uniqueCols;
    }

    public function getTableConstraints(string $schemaName, string $tableName, string $type = 'FOREIGN KEY'): array
    {
        $rows = $this->fetchAll(sprintf(
            "SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND CONSTRAINT_TYPE = '%s'",
            $schemaName,
            $tableName,
            $type
        ));
        return $rows;
    }

    public function query($sql, array $bind = [])
    {
        $stmt = odbc_prepare($this->connection, $sql);
        odbc_execute($stmt, $this->repairBinding($bind));
        odbc_free_result($stmt);
    }

    public function fetchAll($sql, $bind = [])
    {
        $stmt = odbc_prepare($this->connection, $sql);
        odbc_execute($stmt, $this->repairBinding($bind));
        $rows = [];
        while ($row = odbc_fetch_array($stmt)) {
            $rows[] = $row;
        }
        odbc_free_result($stmt);
        return $rows;
    }

    public function fetch($sql, $bind, callable $callback)
    {
        $stmt = odbc_prepare($this->connection, $sql);
        odbc_execute($stmt, $this->repairBinding($bind));
        while ($row = odbc_fetch_array($stmt)) {
            $callback($row);
        }
        odbc_free_result($stmt);
    }

    /**
     * Avoid odbc file open http://php.net/manual/en/function.odbc-execute.php
     * @param array $bind
     * @return array
     */
    private function repairBinding(array $bind)
    {
        return array_map(function ($value) {
            if (preg_match("/^'.*'$/", $value)) {
                return " {$value} ";
            } else {
                return $value;
            }
        }, $bind);
    }
}
