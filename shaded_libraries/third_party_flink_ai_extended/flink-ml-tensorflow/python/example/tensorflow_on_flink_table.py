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

import os

from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, CsvTableSink, WriteMode
from pyflink.table.table import TableSchema

from flink_ml_tensorflow.tensorflow_TFConfig import TFConfig
from flink_ml_tensorflow.tensorflow_on_flink_table import train, inference
from flink_ml_tensorflow.tensorflow_on_flink_tfconf import TFCONSTANS
from flink_ml_tensorflow.tensorflow_on_flink_mlconf import MLCONSTANTS
from pyflink.table.sources import CsvTableSource
from pyflink.table.types import DataTypes


class TableExample:

    @staticmethod
    def add_train_table():
        stream_env = StreamExecutionEnvironment.get_execution_environment()
        table_env = StreamTableEnvironment.create(stream_env)
        statement_set = table_env.create_statement_set()

        work_num = 2
        ps_num = 1
        python_file = os.getcwd() + "/../../src/test/python/add.py"
        func = "map_func"
        prop = {MLCONSTANTS.PYTHON_VERSION: '3.7'}
        env_path = None

        input_tb = None
        output_schema = None

        tf_config = TFConfig(work_num, ps_num, prop, python_file, func, env_path)

        train(stream_env, table_env, statement_set, input_tb, tf_config, output_schema)

        # inference(stream_env, table_env, statement_set, input_tb, tf_config, output_schema)

        job_client = statement_set.execute().get_job_client()
        if job_client is not None:
            job_client.get_job_execution_result(user_class_loader=None).result()

    @staticmethod
    def add_train_chief_alone_table():
        stream_env = StreamExecutionEnvironment.get_execution_environment()
        table_env = StreamTableEnvironment.create(stream_env)
        statement_set = table_env.create_statement_set()
        work_num = 2
        ps_num = 1
        python_file = os.getcwd() + "/../../src/test/python/add.py"
        func = "map_func"
        prop = {}
        prop[TFCONSTANS.TF_IS_CHIEF_ALONE] = "true"
        prop[MLCONSTANTS.PYTHON_VERSION] = "3.7"
        env_path = None
        input_tb = None
        output_schema = None

        tf_config = TFConfig(work_num, ps_num, prop, python_file, func, env_path)

        train(stream_env, table_env, statement_set, input_tb, tf_config, output_schema)

        # inference(stream_env, table_env, statement_set, input_tb, tf_config, output_schema)

        job_client = statement_set.execute().get_job_client()
        if job_client is not None:
            job_client.get_job_execution_result(user_class_loader=None).result()

    @staticmethod
    def input_output_table():
        stream_env = StreamExecutionEnvironment.get_execution_environment()
        table_env = StreamTableEnvironment.create(stream_env)
        statement_set = table_env.create_statement_set()
        work_num = 2
        ps_num = 1
        python_file = os.getcwd() + "/../../src/test/python/input_output.py"
        prop = {}
        func = "map_func"
        env_path = None
        prop[MLCONSTANTS.ENCODING_CLASS] = "com.alibaba.flink.ml.operator.coding.RowCSVCoding"
        prop[MLCONSTANTS.DECODING_CLASS] = "com.alibaba.flink.ml.operator.coding.RowCSVCoding"
        inputSb = "INT_32" + "," + "INT_64" + "," + "FLOAT_32" + "," + "FLOAT_64" + "," + "STRING"
        prop["sys:csv_encode_types"] = inputSb
        prop["sys:csv_decode_types"] = inputSb
        prop[MLCONSTANTS.PYTHON_VERSION] = "3.7"
        source_file = os.getcwd() + "/../../src/test/resources/input.csv"
        sink_file = os.getcwd() + "/../../src/test/resources/output.csv"
        table_source = CsvTableSource(source_file,
                                      ["a", "b", "c", "d", "e"],
                                      [DataTypes.INT(),
                                       DataTypes.BIGINT(),
                                       DataTypes.FLOAT(),
                                       DataTypes.DOUBLE(),
                                       DataTypes.STRING()])
        table_env.register_table_source("source", table_source)
        input_tb = table_env.from_path("source")
        output_schema = TableSchema(["a", "b", "c", "d", "e"],
                                    [DataTypes.INT(),
                                     DataTypes.BIGINT(),
                                     DataTypes.FLOAT(),
                                     DataTypes.DOUBLE(),
                                     DataTypes.STRING()]
                                    )
        sink = CsvTableSink(["a", "b", "c", "d", "e"],
                            [DataTypes.INT(),
                             DataTypes.BIGINT(),
                             DataTypes.FLOAT(),
                             DataTypes.DOUBLE(),
                             DataTypes.STRING()],
                            sink_file,
                            write_mode=WriteMode.OVERWRITE)
        table_env.register_table_sink("table_row_sink", sink)
        tf_config = TFConfig(work_num, ps_num, prop, python_file, func, env_path)
        output_table = train(stream_env, table_env, statement_set, input_tb, tf_config, output_schema)

        # output_table = inference(stream_env, table_env, statement_set, input_tb, tf_config, output_schema)

        statement_set.add_insert("table_row_sink", output_table)
        job_client = statement_set.execute().get_job_client()
        if job_client is not None:
            job_client.get_job_execution_result(user_class_loader=None).result()

    @staticmethod
    def worker_zero_finish():
        stream_env = StreamExecutionEnvironment.get_execution_environment()
        table_env = StreamTableEnvironment.create(stream_env)
        statement_set = table_env.create_statement_set()
        work_num = 3
        ps_num = 2
        python_file = os.getcwd() + "/../../src/test/python/worker_0_finish.py"
        func = "map_func"
        prop = {MLCONSTANTS.PYTHON_VERSION: '3.7'}
        env_path = None
        input_tb = None
        output_schema = None

        tf_config = TFConfig(work_num, ps_num, prop, python_file, func, env_path)
        train(stream_env, table_env, statement_set, input_tb, tf_config, output_schema)

        # inference(stream_env, table_env, statement_set, input_tb, tf_config, output_schema)

        job_client = statement_set.execute().get_job_client()
        if job_client is not None:
            job_client.get_job_execution_result(user_class_loader=None).result()


if __name__ == '__main__':
    TableExample.add_train_table()
    TableExample.add_train_chief_alone_table()
    TableExample.input_output_table()
    TableExample.worker_zero_finish()
