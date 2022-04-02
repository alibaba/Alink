#!/bin/bash

set -xe

SCRIPTPATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"
PY_ROOT="$(
  cd "$SCRIPTPATH/../"
  pwd -P
)"

if [ $# -lt 1 ]
then
  exec pytest --forked -n 8 "$PY_ROOT"/pyalink/alink/tests --html=report.html --self-contained-html
elif [ "--skip-examples" = "$1" ]
then
  exec pytest --forked -n 8 --ignore "$PY_ROOT"/pyalink/alink/tests/examples "$PY_ROOT"/pyalink/alink/tests --html=report.html --self-contained-html
fi
