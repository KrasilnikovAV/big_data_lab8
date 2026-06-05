#!/usr/bin/env bash
set -euo pipefail

IMAGE="${IMAGE:-big-data-lab8-k8s:latest}"
MINIKUBE_PROFILE="${MINIKUBE_PROFILE:-minikube}"

if command -v minikube >/dev/null 2>&1 && minikube -p "${MINIKUBE_PROFILE}" status >/dev/null 2>&1; then
  eval "$(minikube -p "${MINIKUBE_PROFILE}" docker-env)"
fi

docker build -t "${IMAGE}" .
echo "Built image: ${IMAGE}"
