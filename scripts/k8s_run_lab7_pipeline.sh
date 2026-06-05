#!/usr/bin/env bash
set -euo pipefail

# Lab 7 pipeline in Kubernetes is the final source -> datamart -> model contour.
./scripts/k8s_build_image.sh
./scripts/k8s_deploy.sh
./scripts/k8s_verify.sh
