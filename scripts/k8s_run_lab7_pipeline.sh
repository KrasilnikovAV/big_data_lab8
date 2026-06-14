#!/usr/bin/env bash
set -euo pipefail

./scripts/k8s_build_image.sh
./scripts/k8s_deploy.sh
./scripts/k8s_verify.sh
