#!/usr/bin/env bash
cd ../python/flink_ml_framework
#sed -i -E 's/^\(import.*_pb2\)/from \. \1/' *pb2*.py
sed -i -E 's/^import node_pb2 as node__pb2/from \. import node_pb2 as node__pb2/' *pb2*.py
rm -rf *.py-E
