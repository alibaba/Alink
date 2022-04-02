#!/usr/bin/env bash

set -xe

SCRIPTPATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"

bash "$SCRIPTPATH"/examples/run_examples.sh

bash "$SCRIPTPATH"/pyflink_tests/run_pyflink_tests.sh

bash "$SCRIPTPATH"/remote_tests/run_remote_tests.sh
