# Copyright 2019 The flink-ai-extended Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =============================================================================

from pyflink.java_gateway import get_gateway


class TFConfig(object):

    def __init__(self, num_worker, num_ps, properties, python_file, func, env_path):
        """

        :param num_worker: the number of TF workers
        :param num_ps: the number of TF PS
        :param properties: TF properties
        :param python_file: the python file, the entry python file
        :param func: the entry function name in the first python file
        :param env_path: the path of env
        """
        self._num_worker = num_worker
        self._num_ps = num_ps
        self._properties = properties
        self._python_file = python_file
        self._func = func
        self._env_path = env_path

    def java_config(self):
        return get_gateway().jvm.com.alibaba.flink.ml.tensorflow.client.TFConfig(self._num_worker,
                                                                                 self._num_ps,
                                                                                 self._properties,
                                                                                 self._python_file,
                                                                                 self._func,
                                                                                 self._env_path)
