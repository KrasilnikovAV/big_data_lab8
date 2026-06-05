#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
DIST_DIR="${ROOT_DIR}/dist"
ARCHIVE_PATH="${DIST_DIR}/big_data_lab8_kubernetes_model.zip"

cd "${ROOT_DIR}"

if [[ ! -x "${ROOT_DIR}/sbt" ]]; then
  curl -fsSL https://raw.githubusercontent.com/paulp/sbt-extras/master/sbt -o "${ROOT_DIR}/sbt"
  chmod +x "${ROOT_DIR}/sbt"
fi

"${ROOT_DIR}/sbt" assembly

mkdir -p "${DIST_DIR}"
rm -f "${ARCHIVE_PATH}"

zip -rq "${ARCHIVE_PATH}" \
  README.md \
  report.md \
  .dockerignore \
  build.sbt \
  Dockerfile \
  docker-compose.yml \
  build_distribution.sh \
  sbt \
  k8s \
  project/build.properties \
  project/plugins.sbt \
  src \
  config \
  scripts \
  app \
  data \
  target/scala-2.12/big-data-lab8-k8s-datamart.jar

echo "Created archive: ${ARCHIVE_PATH}"
