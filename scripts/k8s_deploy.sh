#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-big-data-lab8}"

wait_for_job() {
  local job_name="$1"
  kubectl wait --for=condition=complete --timeout=20m "job/${job_name}" -n "${NAMESPACE}"
}

restart_job() {
  local manifest="$1"
  local job_name="$2"

  kubectl delete job "${job_name}" -n "${NAMESPACE}" --ignore-not-found
  kubectl apply -f "${manifest}"
  wait_for_job "${job_name}"
}

kubectl apply -f k8s/00-namespace-rbac.yaml
kubectl apply -f k8s/01-mysql.yaml

kubectl rollout status statefulset/mysql -n "${NAMESPACE}" --timeout=10m
kubectl wait --for=condition=ready pod/mysql-0 -n "${NAMESPACE}" --timeout=10m

restart_job k8s/06-lab7-source-loader-job.yaml source-loader-submit
restart_job k8s/07-lab7-datamart-job.yaml datamart-submit
restart_job k8s/08-lab7-model-job.yaml model-submit

kubectl get pods -n "${NAMESPACE}"
