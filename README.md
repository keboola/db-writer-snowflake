# Snowflake DB Writer

[![Build Status](https://travis-ci.org/keboola/db-writer-snowflake.svg?branch=master)](https://travis-ci.org/keboola/db-writer-snowflake)
[![Code Climate](https://codeclimate.com/github/keboola/db-writer-snowflake/badges/gpa.svg)](https://codeclimate.com/github/keboola/db-writer-snowflake)
[![Test Coverage](https://codeclimate.com/github/keboola/db-writer-snowflake/badges/coverage.svg)](https://codeclimate.com/github/keboola/db-writer-snowflake/coverage)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/keboola/db-writer-snowflake/blob/master/LICENSE.md)

Writes data to Redshift Database.

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

App is developed on localhost using TDD.

1. Clone from repository: `git clone git@github.com:keboola/db-writer-snowflake.git`
2. Change directory: `cd db-writer-snowflake`
3. Install dependencies: `docker-compose run --rm php composer install -n`
4. Create `.env` file:
```bash
STORAGE_API_TOKEN=
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
