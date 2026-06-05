#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-big-data-lab8}"

kubectl exec -n "${NAMESPACE}" statefulset/mysql -- \
  mysql -ulab8 -plab8 big_data_lab8 \
  -e "SELECT COUNT(*) AS source_rows FROM product_nutrition; SELECT COUNT(*) AS mart_rows FROM dm_product_nutrition; SELECT run_id, rows_in_source, rows_in_mart, dropped_rows FROM dm_refresh_log ORDER BY created_at DESC LIMIT 3; SELECT run_id, cluster_id, cluster_count, ROUND(silhouette, 4) AS silhouette FROM dm_kmeans_clusters ORDER BY created_at DESC, cluster_id LIMIT 10;"
