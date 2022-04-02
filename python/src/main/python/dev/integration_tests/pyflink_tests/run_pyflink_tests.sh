#!/usr/bin/env bash

set -xe

SCRIPTPATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"

PY_ROOT="$(
  cd "$SCRIPTPATH/../../../"
  pwd -P
)"

function setup_env() {
  FLINK_HOME=$(python3 -c 'import pyflink;print(pyflink.__path__[0])')
  rsync -Pav "$PY_ROOT"/pyalink/lib/alink_*.jar "$FLINK_HOME"/lib/
}

function clean_env() {
  FLINK_HOME=$(python3 -c 'import pyflink;print(pyflink.__path__[0])')
  rm -rf "$FLINK_HOME"/lib/alink_*.jar
}

trap clean_env EXIT

setup_env

for file in "$SCRIPTPATH"/*.py; do
  echo "Testing $file..."
  python3 "$file"
done
