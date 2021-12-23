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
from enum import unique, Enum
from pyflink.util.type_util import TypesUtil
from pyflink.table.table_source import JavaTableSource


@unique
class ScalarConverter(Enum):
    FIRST = 1
    LAST = 2
    MAX = 3
    MIN = 4
    ONE_HOT = 5

    def java_converter(self):
        if self is ScalarConverter.FIRST:
            return get_gateway().jvm.com.alibaba.flink.tensorflow.hadoop.io.TFRExtractRowHelper.ScalarConverter.FIRST
        if self is ScalarConverter.LAST:
            return get_gateway().jvm.com.alibaba.flink.tensorflow.hadoop.io.TFRExtractRowHelper.ScalarConverter.LAST
        if self is ScalarConverter.MAX:
            return get_gateway().jvm.com.alibaba.flink.tensorflow.hadoop.io.TFRExtractRowHelper.ScalarConverter.MAX
        if self is ScalarConverter.MIN:
            return get_gateway().jvm.com.alibaba.flink.tensorflow.hadoop.io.TFRExtractRowHelper.ScalarConverter.MIN
        if self is ScalarConverter.ONE_HOT:
            return get_gateway().jvm.com.alibaba.flink.tensorflow.hadoop.io.TFRExtractRowHelper.ScalarConverter.ONE_HOT
        raise Exception('Unknown converter ' + self.name)


class TFRTableSource(JavaTableSource):
    def __init__(self, paths, epochs, out_row_type, converters):
        table_src_clz_name = 'com.alibaba.flink.tensorflow.hadoop.io.TFRToRowTableSource'
        table_src_clz = TypesUtil.class_for_name(table_src_clz_name)
        j_paths = TypesUtil._convert_py_list_to_java_array('java.lang.String', paths)
        j_converters = []
        for converter in converters:
            j_converters.append(converter.java_converter())
        j_converters = TypesUtil._convert_py_list_to_java_array(
            'com.alibaba.flink.tensorflow.hadoop.io.TFRExtractRowHelper$ScalarConverter', j_converters)
        j_row_type = TypesUtil.to_java_sql_type(out_row_type)
        super(TFRTableSource, self).__init__(table_src_clz(j_paths, epochs, j_row_type, j_converters))

    def register_table(self, table_env, name=None):
        if name is None:
            import time
            name = 'tfr_table_src_' + str(int(round(time.time() * 1000)))
        table_env.register_table_source(name=name, table_source=self)
        return table_env.scan(name)
