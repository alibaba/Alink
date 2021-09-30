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

import json
import os
import tensorflow as tf
from flink_ml_tensorflow2 import tensorflow_on_flink_ops as flink_ops
from flink_ml_framework.context import Context


class TFContext(Context):
    def __init__(self, other):
        if isinstance(other, Context):
            self.__dict__ = other.__dict__.copy()

    @staticmethod
    def to_tf_cluster(cluster_str):
        cluster_json = json.loads(cluster_str)
        tf_cluster = {'ps': [], 'worker': []}
        jobs = cluster_json['job']
        for job in jobs:
            if 'worker' == job['name']:
                worker_num = len(job['tasks'])
                for i in range(worker_num):
                    task = job['tasks'][str(i)]
                    ip = task['ip']
                    port = task['props']['sys:tf_port']
                    address = ip + ":" + port
                    tf_cluster['worker'].append(address)
                continue
            if 'ps' == job['name']:
                ps_num = len(job['tasks'])
                for i in range(ps_num):
                    task = job['tasks'][str(i)]
                    ip = task['ip']
                    port = task['props']['sys:tf_port']
                    address = ip + ":" + port
                    tf_cluster['ps'].append(address)
        if 0 == len(tf_cluster['ps']):
            del tf_cluster['ps']
        return tf_cluster

    def get_tf_cluster(self):
        cluster_str = self.get_property("cluster")
        return TFContext.to_tf_cluster(cluster_str)

    @staticmethod
    def cluster_to_estimator(cluster_str):
        cluster = TFContext.to_tf_cluster(cluster_str)
        worker_0 = cluster['worker'][0]
        del (cluster['worker'][0])
        if 0 == len(cluster['worker']):
            del (cluster['worker'])
        cluster['chief'] = [worker_0]
        return cluster

    @staticmethod
    def export_cluster_env(cluster_str, job_name, index):
        cluster = TFContext.cluster_to_estimator(cluster_str)
        if 'ps' == job_name:
            task_type = 'ps'
            task_index = index
        elif 'worker' == job_name:
            if 0 == index:
                task_type = 'chief'
                task_index = 0
            else:
                task_type = 'worker'
                task_index = index - 1

        os.environ['TF_CONFIG'] = json.dumps(
            {'cluster': cluster,
             'task': {'type': task_type, 'index': task_index}})
        print (os.environ['TF_CONFIG'])
        return cluster, task_type, task_index

    def export_estimator_cluster(self):
        cluster_str = self.properties["cluster"]
        return TFContext.export_cluster_env(cluster_str, self.roleName, self.index)

    def example_input_dataset(self):
        dataset = tf.data.TFRecordDataset(self.from_java())
        dataset = dataset.map(lambda record: tf.parse_single_example(record, features=self.features))
        return dataset

    def output_writer_op(self, input_list):
        path = self.to_java()
        writer = flink_ops.FlinkTFRecordWriter(address=path)
        write_op = writer.write(input_list)
        close_op = writer.close()
        return write_op, close_op

    def flink_stream_dataset(self, compression_type=None, buffer_size=0, num_parallel_reads=None):
        return flink_ops.FlinkStreamDataSet(self.from_java(), compression_type, buffer_size, num_parallel_reads)
