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
distPath=${basePath}/dist
if [[ ! -d ${distPath} ]]; then
  mkdir ${distPath}
fi

frameworkPath=${basePath}/../flink-ml-framework/python/
cd ${frameworkPath}
python setup.py bdist_wheel
cp ${frameworkPath}/dist/* ${distPath}/
rm -rf ${frameworkPath}/build/ ${frameworkPath}/dist/ ${frameworkPath}/flink_ml_framework.egg-info

tensorflowPath=${basePath}/../flink-ml-tensorflow/python/
cd ${tensorflowPath}
python setup.py bdist_wheel
cp ${tensorflowPath}/dist/* ${distPath}/
rm -rf ${tensorflowPath}/build/ ${tensorflowPath}/dist/ ${tensorflowPath}/flink_ml_tensorflow.egg-info
