#!/bin/bash
set -e

export DBT_KEY="$DBT_KEY"

dbt run --profiles-dir /app
