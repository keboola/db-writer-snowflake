<?php

declare(strict_types=1);

namespace Keboola\DbWriter\Adapter;

use Keboola\DbWriter\Writer\Snowflake;

class S3Adapter implements IAdapter
{
    private bool $isSliced;

    private string $region;

    private string $bucket;

    private string $key;

    private string $accessKeyId;

    private string $secretAccessKey;

    private string $sessionToken;

    public function __construct(array $s3info)
    {
        $this->isSliced = $s3info['isSliced'];
        $this->region = $s3info['region'];
        $this->bucket = $s3info['bucket'];
        $this->key = $s3info['key'];
        $this->accessKeyId = $s3info['credentials']['access_key_id'];
        $this->secretAccessKey = $s3info['credentials']['secret_access_key'];
        $this->sessionToken = $s3info['credentials']['session_token'];
    }

    public function generateCreateStageCommand(string $stageName): string
    {
        $csvOptions = [];
        $csvOptions[] = sprintf('FIELD_DELIMITER = %s', Snowflake::quote(','));
        $csvOptions[] = sprintf('FIELD_OPTIONALLY_ENCLOSED_BY = %s', Snowflake::quote('"'));
        $csvOptions[] = sprintf('ESCAPE_UNENCLOSED_FIELD = %s', Snowflake::quote('\\'));

        if (!$this->isSliced) {
            $csvOptions[] = 'SKIP_HEADER = 1';
        }

        return sprintf(
            "CREATE OR REPLACE STAGE %s
             FILE_FORMAT = (TYPE=CSV %s)
             URL = 's3://%s'
             CREDENTIALS = (AWS_KEY_ID = %s AWS_SECRET_KEY = %s  AWS_TOKEN = %s)
            ",
            Snowflake::quoteIdentifier($stageName),
            implode(' ', $csvOptions),
            $this->bucket,
            Snowflake::quote($this->accessKeyId),
            Snowflake::quote($this->secretAccessKey),
            Snowflake::quote($this->sessionToken)
        );
    }

    public function generateCopyCommand(string $tableName, string $stageName, array $columns): string
    {
        $columnNames = array_map(function ($column) {
            return Snowflake::quoteIdentifier($column['dbName']);
        }, $columns);

        $transformationColumns = array_map(
            function ($column, $index) {
                if (!empty($column['nullable'])) {
                    return sprintf("IFF(t.$%d = '', null, t.$%d)", $index + 1, $index + 1);
                }
                return sprintf('t.$%d', $index + 1);
            },
            $columns,
            array_keys($columns)
        );

        $path = $this->key;
        $pattern = '';
        if ($this->isSliced) {
            // key ends with manifest
            if (strrpos($this->key, 'manifest') === strlen($this->key) - strlen('manifest')) {
                $path = substr($this->key, 0, strlen($this->key) - strlen('manifest'));
                $pattern = 'PATTERN="^.*(?<!manifest)$"';
            }
        }

        return sprintf(
            'COPY INTO %s(%s) 
            FROM (SELECT %s FROM %s t)
            %s',
            $tableName,
            implode(', ', $columnNames),
            implode(', ', $transformationColumns),
            Snowflake::quote('@' . Snowflake::quoteIdentifier($stageName) . '/' . $path),
            $pattern
        );
    }
}
