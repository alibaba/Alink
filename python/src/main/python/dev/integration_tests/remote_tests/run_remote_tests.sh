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

NUM_TM=2

# Obtain a free port for Flink WebUI
get_free_port_py="
import socket
with socket.socket() as s:
  s.bind(('', 0))
  print(s.getsockname()[1])
"

free_port=$(python3 -c "$get_free_port_py")

function setup_env() {
  FLINK_HOME=$(python3 -c 'import pyflink;print(pyflink.__path__[0])')
  sed -i "s/\#\?rest\.port: [[:digit:]]\+/rest.port: $free_port/g" "$FLINK_HOME"/conf/flink-conf.yaml
  rsync -Pav "$PY_ROOT"/pyalink/lib/alink_*.jar "$FLINK_HOME"/lib/
  for i in $(seq $NUM_TM); do
    "$FLINK_HOME"/bin/start-cluster.sh
  done
}

function clean_env() {
  FLINK_HOME=$(python3 -c 'import pyflink;print(pyflink.__path__[0])')
  for i in $(seq $NUM_TM); do
    "$FLINK_HOME"/bin/stop-cluster.sh
  done
  rm -rf "$FLINK_HOME"/lib/alink_*.jar
  sed -i 's/\#\?rest\.port: [[:digit:]]\+/#rest.port: 8081/g' "$FLINK_HOME"/conf/flink-conf.yaml
}

trap clean_env EXIT

setup_env

function test_with_remote_env() {
  host=$1
  port=$2
  local_ip=$3

  for file in "$SCRIPTPATH"/*.py; do
    echo "Testing $file..."
    python3 "$file" "$host" "$port" "$local_ip"
  done
}

test_with_remote_env localhost "$free_port" localhost
