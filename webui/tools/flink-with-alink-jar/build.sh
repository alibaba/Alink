#!/bin/sh

set -e

SCRIPTPATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"

echo "Build docker image in ${DOCKER_FILE_PATH}"

docker build -t flink_alink:v0.1 -f "${SCRIPTPATH}"/Dockerfile "${SCRIPTPATH}"
