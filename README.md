# Snowflake DB Writer

[![Build Status](https://travis-ci.com/keboola/db-writer-snowflake.svg?branch=master)](https://travis-ci.com/keboola/db-writer-snowflake)
[![Code Climate](https://codeclimate.com/github/keboola/db-writer-snowflake/badges/gpa.svg)](https://codeclimate.com/github/keboola/db-writer-snowflake)
[![Test Coverage](https://codeclimate.com/github/keboola/db-writer-snowflake/badges/coverage.svg)](https://codeclimate.com/github/keboola/db-writer-snowflake/coverage)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/keboola/db-writer-snowflake/blob/master/LICENSE.md)

Writes data to Snowflake Database.

## Example configuration

```json
    {
      "db": {        
        "host": "HOST",
        "port": "PORT",
        "database": "DATABASE",
        "user": "USERNAME",
        "password": "PASSWORD",
        "schema": "SCHEMA",
        "warehouse": "WAREHOUSE",
        "ssh": {
          "enabled": true,
          "keys": {
            "private": "ENCRYPTED_PRIVATE_SSH_KEY",
            "public": "PUBLIC_SSH_KEY"
          },
          "sshHost": "PROXY_HOSTNAME"
        }
      },
      "tables": [
        {
          "tableId": "simple",
          "dbName": "simple",
          "export": true, 
          "incremental": true,
          "primaryKey": ["id"],
          "items": [
            {
              "name": "id",
              "dbName": "id",
              "type": "int",
              "size": null,
              "nullable": null,
              "default": null
            },
            {
              "name": "name",
              "dbName": "name",
              "type": "nvarchar",
              "size": 255,
              "nullable": null,
              "default": null
            },
            {
              "name": "glasses",
              "dbName": "glasses",
              "type": "nvarchar",
              "size": 255,
              "nullable": null,
              "default": null
            }
          ]                                
        }
      ]
    }
```

## Development

Required snowflake resource for writer:
```sql
CREATE DATABASE "snowflake_writer";
USE DATABASE "snowflake_writer";
CREATE TRANSIENT SCHEMA "snowflake_writer";
CREATE WAREHOUSE "snowflake_writer" WITH 
  WAREHOUSE_SIZE = 'XSMALL' 
  WAREHOUSE_TYPE = 'STANDARD' 
  AUTO_SUSPEND = 900 
  AUTO_RESUME = TRUE;
CREATE ROLE "snowflake_writer";
GRANT USAGE ON WAREHOUSE "snowflake_writer" TO ROLE "snowflake_writer";
GRANT USAGE ON DATABASE "snowflake_writer" TO ROLE "snowflake_writer";
GRANT ALL ON SCHEMA "snowflake_writer" TO ROLE "snowflake_writer";
GRANT ALL ON FUTURE TABLES IN SCHEMA "snowflake_writer" TO ROLE "snowflake_writer";
GRANT ALL ON FUTURE VIEWS IN SCHEMA "snowflake_writer" TO ROLE "snowflake_writer";
CREATE USER "snowflake_writer" 
  PASSWORD = 'password' 
  DEFAULT_ROLE = "snowflake_writer" 
  DEFAULT_WAREHOUSE = "snowflake_writer" 
  DEFAULT_NAMESPACE = "snowflake_writer"."snowflake_writer" 
  MUST_CHANGE_PASSWORD = FALSE;
GRANT ROLE "snowflake_writer" TO USER "snowflake_writer";
```

App is developed on localhost using TDD.

1. Clone from repository: `git clone git@github.com:keboola/db-writer-snowflake.git`
2. Change directory: `cd db-writer-snowflake`
3. Install dependencies: `docker-compose run --rm php composer install -n`
4. Create `.env` file:
```bash
STORAGE_API_TOKEN=
KBC_URL=
KBC_RUNID=
DB_HOST=
DB_PORT=
DB_DATABASE=
DB_USER=
DB_PASSWORD=
DB_SCHEMA=
DB_WAREHOUSE=
```
5. Run docker-compose, which will trigger phpunit: `docker-compose run --rm app`
