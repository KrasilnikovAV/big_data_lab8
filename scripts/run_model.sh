#!/usr/bin/env bash
set -euo pipefail

exec /opt/spark/bin/spark-submit \
  --jars "${MYSQL_CONNECTOR_JAR}" \
  app/kmeans_datamart.py

