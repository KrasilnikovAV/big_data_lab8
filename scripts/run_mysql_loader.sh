#!/usr/bin/env bash
set -euo pipefail

exec /opt/spark/bin/spark-submit \
  --jars "${MYSQL_CONNECTOR_JAR}" \
  app/load_openfoodfacts_to_mysql.py

