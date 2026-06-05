#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-big-data-lab8}"

kubectl delete namespace "${NAMESPACE}" --ignore-not-found
