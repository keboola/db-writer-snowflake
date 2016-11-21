#!/bin/bash

docker login -u="$QUAY_USERNAME" -p="$QUAY_PASSWORD" quay.io
docker tag keboola/db-writer-snowflake quay.io/keboola/db-writer-snowflake:$TRAVIS_TAG
docker images
docker push quay.io/keboola/db-writer-snowflake:$TRAVIS_TAG
