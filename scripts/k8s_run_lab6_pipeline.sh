#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-big-data-lab8}"
IMAGE="${LAB6_IMAGE:-big-data-lab6-model:latest}"

docker build -t "${IMAGE}" ../big_data_lab6

kubectl apply -f k8s/00-namespace-rbac.yaml
kubectl apply -f k8s/01-mysql.yaml
kubectl rollout status statefulset/mysql -n "${NAMESPACE}" --timeout=10m
kubectl wait --for=condition=ready pod/mysql-0 -n "${NAMESPACE}" --timeout=10m

kubectl delete job lab6-source-loader-submit -n "${NAMESPACE}" --ignore-not-found
kubectl delete job lab6-model-submit -n "${NAMESPACE}" --ignore-not-found

kubectl apply -f k8s/04-lab6-source-loader-job.yaml
kubectl wait --for=condition=complete job/lab6-source-loader-submit -n "${NAMESPACE}" --timeout=20m

kubectl apply -f k8s/05-lab6-model-job.yaml
kubectl wait --for=condition=complete job/lab6-model-submit -n "${NAMESPACE}" --timeout=20m

kubectl exec -n "${NAMESPACE}" statefulset/mysql -- \
  mysql -ulab8 -plab8 big_data_lab8 \
  -e "SELECT COUNT(*) AS source_rows FROM product_nutrition; SELECT run_id, cluster_id, cluster_count, ROUND(silhouette, 4) AS silhouette FROM kmeans_clusters ORDER BY created_at DESC, cluster_id LIMIT 10;"
