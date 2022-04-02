#!/usr/bin/env bash

set -xe

SCRIPTPATH="$(
  cd "$(dirname "$0")"
  pwd -P
)"

for file in $SCRIPTPATH/*.py; do
  echo "Testing $file..."
  python3 "$file"
done
