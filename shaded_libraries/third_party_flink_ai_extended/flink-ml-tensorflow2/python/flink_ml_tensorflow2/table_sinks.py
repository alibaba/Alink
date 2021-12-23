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
from pyflink.util.type_util import TypesUtil
from pyflink.table.sinks import JavaTableSink
from sink_funcs import LogSink


class LogTableStreamSink(JavaTableSink):
    def __init__(self, sink_func=None):
        if sink_func is None:
            sink_func = LogSink()._j_sink_function
        sink_clz_name = 'com.alibaba.flink.tensorflow.flink_op.sink.LogTableStreamSink'
        sink_clz = TypesUtil.class_for_name(sink_clz_name)
        super(LogTableStreamSink, self).__init__(sink_clz(sink_func))


class LogInferAccSink(LogTableStreamSink):
    def __init__(self):
        sink_func = get_gateway().jvm.com.alibaba.flink.tensorflow.client.LogInferAccSink()
        super(LogInferAccSink, self).__init__(sink_func=sink_func)
