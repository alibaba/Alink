#!/bin/bash
##
## Licensed to the Apache Software Foundation (ASF) under one
## or more contributor license agreements.  See the NOTICE file
## distributed with this work for additional information
## regarding copyright ownership.  The ASF licenses this file
## to you under the Apache License, Version 2.0 (the
## "License"); you may not use this file except in compliance
## with the License.  You may obtain a copy of the License at
##
##   http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing,
## software distributed under the License is distributed on an
## "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
## KIND, either express or implied.  See the License for the
## specific language governing permissions and limitations
## under the License.
##
basePath=$(cd `dirname $0`; pwd)
set -e
if [[ 1 -ne $# ]]; then
    echo "usage: create_venv_package.sh sourcePath"
    exit 1
fi
sourcePath=$1
echo "source path:${sourcePath}"

virtualenv ${basePath}/tfenv
cd ${basePath}
. tfenv/bin/activate
pip install grpcio six numpy wheel mock keras_applications keras_preprocessing
pip install tensorflow==1.11.0
cd ${sourcePath}/flink-ml-framework/python
pip install .
cd ${sourcePath}/flink-ml-tensorflow/python
pip install .
cd ${basePath}
deactivate
touch ${basePath}/tfenv/lib/python2.7/site-packages/google/__init__.py

cd ${basePath}
zip -ryq tfenv.zip tfenv
rm -rf tfenv
