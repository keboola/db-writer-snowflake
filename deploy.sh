#!/bin/bash

docker login -u="$QUAY_USERNAME" -p="$QUAY_PASSWORD" quay.io
docker tag dbwritersnowflake_app quay.io/keboola/db-writer-snowflake:$TRAVIS_TAG
docker push quay.io/keboola/db-writer-snowflake:$TRAVIS_TAG
docker push quay.io/keboola/db-writer-snowflake:latest
