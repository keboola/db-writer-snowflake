#!/bin/bash
set -e

# writer with aws s3 storage backend
export KBC_DEVELOPERPORTAL_APP=keboola.wr-db-snowflake
./deploy.sh

# writer with azure blob storage backend
export KBC_DEVELOPERPORTAL_APP=keboola.wr-snowflake-blob-storage
./deploy.sh
