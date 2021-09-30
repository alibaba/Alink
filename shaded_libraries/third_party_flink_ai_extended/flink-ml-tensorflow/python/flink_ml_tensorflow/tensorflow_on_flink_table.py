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
from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table.table import Table


def inference(stream_env=None, table_env=None, statement_set=None, input_table=None, tf_config=None, output_schema=None):
    """
    Run TF inference for flink table api.

    :param stream_env: The StreamExecutionEnvironment. If it's None, this method will create one and execute the job
                       at the end. Otherwise, caller is responsible to trigger the job execution.
    :param table_env: The Flink TableEnvironment.
    :param statement_set: The StatementSet created by the given TableEnvironment.
    :param input_table: The input Table.
    :param tf_config: Configurations for the TF program.
    :param output_schema: The TableSchema for the output Table. If it's null, a dummy sink will be connected.
    :return: output Table. Otherwise, caller is responsible to add sink to the output Table before executing the graph.
    """

    if stream_env is None:
        stream_env = StreamExecutionEnvironment.get_execution_environment()
    if table_env is None:
        table_env = StreamTableEnvironment.create(stream_env)
    if statement_set is None:
        statement_set = table_env.create_statement_set()
    if input_table is not None:
        input_table = input_table._j_table
    if output_schema is not None:
        output_schema = output_schema._j_table_schema
    output_table = get_gateway().jvm.com.alibaba.flink.ml.tensorflow.client.TFUtils.inference(
        stream_env._j_stream_execution_environment,
        table_env._j_tenv,
        statement_set._j_statement_set,
        input_table,
        tf_config.java_config(),
        output_schema)
    return Table(output_table, table_env)


def train(stream_env=None, table_env=None, statement_set=None, input_table=None, tf_config=None, output_schema=None):
    """
    Run TF train for flink table api.

    :param stream_env: The StreamExecutionEnvironment. If it's None, this method will create one and execute the job
                       at the end. Otherwise, caller is responsible to trigger the job execution
    :param table_env: The Flink TableEnvironment.
    :param statement_set: The StatementSet created by the given TableEnvironment.
    :param input_table: The input Table.
    :param tf_config: Configurations for the TF program.
    :param output_schema: The TableSchema for the output Table. If it's null, a dummy sink will be connected.
    :return: output Table. Otherwise, caller is responsible to add sink to the output Table before executing the graph.
    """

    if stream_env is None:
        stream_env = StreamExecutionEnvironment.get_execution_environment()
    if table_env is None:
        table_env = StreamTableEnvironment.create(stream_env)
    if statement_set is None:
        statement_set = table_env.create_statement_set()
    if input_table is not None:
        input_table = input_table._j_table
    if output_schema is not None:
        output_schema = output_schema._j_table_schema
    output_table = get_gateway().jvm.com.alibaba.flink.ml.tensorflow.client.TFUtils.train(
        stream_env._j_stream_execution_environment,
        table_env._j_tenv,
        statement_set._j_statement_set,
        input_table,
        tf_config.java_config(),
        output_schema)
    return Table(output_table,table_env)
