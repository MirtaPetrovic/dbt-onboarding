#!/bin/bash
set -e

echo -e "$DBT_KEY" > /tmp/keyfile.json

dbt run --profiles-dir /app
