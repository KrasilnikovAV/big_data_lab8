#!/usr/bin/env bash
set -euo pipefail

exec /opt/spark/bin/spark-submit \
  --jars "${MYSQL_CONNECTOR_JAR}" \
  --class DataMartApp \
  target/scala-2.12/big-data-lab8-k8s-datamart.jar \
  --mysql-url "${MYSQL_URL}" \
  --mysql-user "${MYSQL_USER}" \
  --mysql-password "${MYSQL_PASSWORD}" \
  --source-table "${MYSQL_SOURCE_TABLE:-product_nutrition}" \
  --mart-table "${MYSQL_MART_TABLE:-dm_product_nutrition}" \
  --refresh-log-table "${MYSQL_REFRESH_LOG_TABLE:-dm_refresh_log}" \
  --output-dir "${OUTPUT_DIR:-outputs/datamart}" \
  --spark-master "${SPARK_MASTER:-local[2]}" \
  --spark-config "${SPARK_CONFIG_PATH:-config/spark-defaults.conf}"
