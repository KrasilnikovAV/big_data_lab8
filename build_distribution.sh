#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
DIST_DIR="${ROOT_DIR}/dist"
ARCHIVE_PATH="${DIST_DIR}/big_data_lab8_kubernetes_model.zip"
JAVA17_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"

cd "${ROOT_DIR}"

if ! command -v sbt >/dev/null 2>&1; then
  echo "sbt is not installed. Install it first, for example: brew install sbt" >&2
  exit 1
fi

if [[ -d "${JAVA17_HOME}" ]]; then
  export JAVA_HOME="${JAVA17_HOME}"
  export PATH="${JAVA_HOME}/bin:${PATH}"
fi

sbt assembly

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
