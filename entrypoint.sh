#!/bin/bash
set -e

if [ -n "$DBT_KEY" ]; then
  echo "$DBT_KEY" > /tmp/keyfile.json
  export DBT_KEY_JSON_PATH="/tmp/keyfile.json"
fi
dbt run --profiles-dir /app
