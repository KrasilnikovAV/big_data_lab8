#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-big-data-lab8}"
IMAGE="${LAB5_IMAGE:-big-data-lab5-model:latest}"

docker build -t "${IMAGE}" ../big_data_lab5

kubectl apply -f k8s/00-namespace-rbac.yaml
kubectl delete job lab5-model-submit -n "${NAMESPACE}" --ignore-not-found
kubectl apply -f k8s/03-lab5-model-job.yaml
kubectl wait --for=condition=complete job/lab5-model-submit -n "${NAMESPACE}" --timeout=20m

driver="$(
  kubectl get pods -n "${NAMESPACE}" --no-headers -o custom-columns=NAME:.metadata.name \
    | grep '^lab5-model-.*-driver$' \
    | tail -1
)"

kubectl logs -n "${NAMESPACE}" "${driver}" | grep -E "Rows|KMeans|Silhouette|Cluster statistics"
