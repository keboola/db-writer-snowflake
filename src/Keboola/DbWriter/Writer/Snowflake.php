<?php
/**
 * Created by PhpStorm.
 * User: miroslavcillik
 * Date: 12/02/16
 * Time: 16:38
 */

namespace Keboola\DbWriter\Writer;

use Aws\Exception\AwsException;
use Aws\S3\S3Client;
use Keboola\Csv\CsvFile;
use Keboola\DbWriter\Exception\ApplicationException;
use Keboola\DbWriter\Exception\UserException;
use Keboola\DbWriter\Logger;
use Keboola\DbWriter\Snowflake\Connection;
use Keboola\DbWriter\Snowflake\Exception;
use Keboola\DbWriter\Writer;
use Keboola\DbWriter\WriterInterface;

class Snowflake extends Writer implements WriterInterface
{
    private static $allowedTypes = [
        'number',
        'decimal', 'numeric',
        'int', 'integer', 'bigint', 'smallint', 'tinyint', 'byteint',
        'float', 'float4', 'float8',
        'double', 'double precision', 'real',
        'boolean',
        'char', 'character', 'varchar', 'string', 'text',
        'date', 'time', 'timestamp', 'timestamp_ltz', 'timestamp_ntz', 'timestamp_tz'
    ];

    private static $typesWithSize = [
        'number', 'decimal', 'numeric',
        'char', 'character', 'varchar', 'string', 'text',
        'date', 'time', 'timestamp', 'timestamp_ltz', 'timestamp_ntz', 'timestamp_tz'
    ];

    /** @var Connection */
    protected $db;

    private $dbParams;

    /** @var Logger */
    protected $logger;

    public function __construct($dbParams, Logger $logger)
    {
        parent::__construct($dbParams, $logger);
        $this->dbParams = $dbParams;
        $this->logger = $logger;
    }

    public function createConnection($dbParams)
    {
        $connection = new Connection($dbParams);
        $connection->query(sprintf('USE SCHEMA "%s"', $dbParams['schema']));

        return $connection;
    }

    public function writeFromS3($s3info, array $table)
    {
        $command = $this->generateCopyCommand($table['dbName'], $s3info);
        try {
            $this->db->query($command);
        } catch (Exception $e) {
            throw new UserException("Import error: " . $e->getMessage(), 0, $e);
        }
    }

    private function generateCopyCommand($tableName, $s3info)
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', $this->quote(','));
        $csvOptions[] = sprintf("FIELD_OPTIONALLY_ENCLOSED_BY = %s", $this->quote('"'));
        $csvOptions[] = "ESCAPE_UNENCLOSED_FIELD = NONE";
        $csvOptions[] = sprintf("ESCAPE_UNENCLOSED_FIELD = %s", $this->quote('\\'));

        if ($s3info['isSliced']) {
            $s3Bucket = 's3://' . $s3info['bucket'];
            $downloadManifest = $this->getManifestDownloader($s3info);
            return sprintf(
                "COPY INTO %s FROM %s
                CREDENTIALS = (AWS_KEY_ID = %s AWS_SECRET_KEY = %s AWS_TOKEN = %s)
                FILE_FORMAT = (TYPE=CSV %s)
                FILES = (%s)",
                $this->nameWithSchemaEscaped($tableName),
                $this->quote($s3Bucket), // s3 bucket
                $this->quote($s3info['credentials']['access_key_id']),
                $this->quote($s3info['credentials']['secret_access_key']),
                $this->quote($s3info['credentials']['session_token']),
                implode(' ', $csvOptions),
                implode(
                    ', ',
                    array_map(
                        function ($file) use ($s3Bucket) {
                            return $this->quote(str_replace($s3Bucket . '/', '', $file));
                        },
                        $downloadManifest($s3info['bucket'], $s3info['key'])
                    )
                )
            );
        } else {
            return sprintf(
                "COPY INTO %s FROM %s
                CREDENTIALS = (AWS_KEY_ID = %s AWS_SECRET_KEY = %s AWS_TOKEN = %s)
                REGION = %s
                FILE_FORMAT = (TYPE=CSV %s)",
                $this->nameWithSchemaEscaped($tableName),
                $this->quote('s3://' . $s3info['bucket'] . "/" . $s3info['key']),
                $this->quote($s3info['credentials']['access_key_id']),
                $this->quote($s3info['credentials']['secret_access_key']),
                $this->quote($s3info['credentials']['session_token']),
                implode(' ', $csvOptions)
            );
        }
    }

    private function getManifestDownloader($s3info)
    {
        $s3Client = new S3Client([
            'credentials' => [
                'key' => $s3info['credentials']['access_key_id'],
                'secret' => $s3info['credentials']['secret_access_key'],
                'token' => $s3info['credentials']['session_token']
            ],
            'region' => $s3info['region'],
            'version' => '2006-03-01',
        ]);

        return function ($bucket, $key) use ($s3Client) {
            try {
                $response = $s3Client->getObject([
                    'Bucket' => $bucket,
                    'Key' => $key,
                ]);
            } catch (AwsException $e) {
                throw new Exception('Unable to download file from S3: ' . $e->getMessage());
            }
            $manifest = json_decode((string)$response['Body'], true);

            return array_map(function ($entry) {
                return $entry['url'];
            }, $manifest['entries']);
        };
    }

    protected function nameWithSchemaEscaped($tableName, $schemaName = null)
    {
        if ($schemaName === null) {
            $schemaName = $this->dbParams['schema'];
        }
        return sprintf(
            '%s.%s',
            $this->quoteIdentifier($schemaName),
            $this->quoteIdentifier($tableName)
        );
    }

    private function quote($value)
    {
        return "'" . addslashes($value) . "'";
    }

    private function quoteIdentifier($value)
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }

    public function isTableValid(array $table, $ignoreExport = false)
    {
        // TODO: Implement isTableValid() method.

        return true;
    }

    public function drop($tableName)
    {
        $this->db->query(sprintf("DROP TABLE IF EXISTS %s;", $this->escape($tableName)));
    }

    public function create(array $table)
    {
        $sql = "CREATE TABLE {$this->escape($table['dbName'])} (";

        $columns = array_filter($table['items'], function ($item) {
            return (strtolower($item['type']) !== 'ignore');
        });
        foreach ($columns as $col) {
            $type = strtoupper($col['type']);
            if (!empty($col['size'])) {
                $type .= "({$col['size']})";
            }
            $null = $col['nullable'] ? 'NULL' : 'NOT NULL';
            $default = empty($col['default']) ? '' : "DEFAULT '{$col['default']}'";
            if ($type == 'TEXT') {
                $default = '';
            }
            $sql .= "{$this->escape($col['dbName'])} $type $null $default";
            $sql .= ',';
        }
        $sql = substr($sql, 0, -1);
        $sql .= ");";

        $this->execQuery($sql);
    }

    public function upsert(array $table, $targetTable)
    {
        $sourceTable = $this->nameWithSchemaEscaped($table['dbName']);
        $targetTable = $this->nameWithSchemaEscaped($targetTable);

        $columns = array_map(
            function ($item) {
                return $this->quoteIdentifier($item['dbName']);
            },
            array_filter($table['items'], function ($item) {
                return strtolower($item['type']) != 'ignore';
            })
        );

        if (!empty($table['primaryKey'])) {
            // update data
            $joinClauseArr = [];
            foreach ($table['primaryKey'] as $index => $value) {
                $joinClauseArr[] = sprintf(
                    '%s.%s=%s.%s',
                    $targetTable,
                    $this->quoteIdentifier($value),
                    $sourceTable,
                    $this->quoteIdentifier($value)
                );
            }
            $joinClause = implode(' AND ', $joinClauseArr);

            $valuesClauseArr = [];
            foreach ($columns as $index => $column) {
                $valuesClauseArr[] = sprintf(
                    '%s=%s.%s',
                    $column,
                    $sourceTable,
                    $column
                );
            }
            $valuesClause = implode(',', $valuesClauseArr);

            $query = "
                UPDATE {$targetTable}
                SET {$valuesClause}
                FROM {$sourceTable}
                WHERE {$joinClause}
            ";

            var_dump($query);

            $this->execQuery($query);

            // delete updated from temp table
            $query = "
                DELETE FROM {$sourceTable}
                USING {$targetTable}
                WHERE {$joinClause}
            ";

            $this->execQuery($query);
        }

        // insert new data
        $columnsClause = implode(',', $columns);
        $query = "INSERT INTO {$targetTable} ({$columnsClause}) SELECT * FROM {$sourceTable}";
        $this->execQuery($query);

        // drop temp table
        $this->drop($table['dbName']);
    }

    public static function getAllowedTypes()
    {
        return self::$allowedTypes;
    }

    public function tableExists($tableName)
    {
        $res = $this->db->fetchAll(sprintf("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '%s'", $tableName));
        return !empty($res);
    }

    private function execQuery($query)
    {
        $this->logger->info(sprintf("Executing query '%s'", $query));
        $this->db->query($query);
    }

    public function showTables($dbName)
    {
        throw new ApplicationException("Method not implemented");
    }

    public function getTableInfo($tableName)
    {
        throw new ApplicationException("Method not implemented");
    }

    public function write(CsvFile $csv, array $table)
    {
        throw new ApplicationException("Method not implemented");
    }

    private function escape($str)
    {
        return '"' . $str . '"';
    }

    public function testConnection()
    {
        $this->db->query('select current_date');
    }

    public function generateTmpName($tableName)
    {
        return '__temp_' . str_replace('.', '_', uniqid('wr-db-snowflake', true));
    }
}
