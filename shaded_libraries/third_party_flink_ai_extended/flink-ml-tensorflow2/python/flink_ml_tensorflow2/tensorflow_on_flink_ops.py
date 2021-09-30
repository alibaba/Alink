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

from tensorflow.python.platform import resource_loader
import tensorflow as tf
import os

queue_library = os.path.join(resource_loader.get_data_files_path(),
                             "libflink_ops.so")
print(queue_library)
try:
    flink_ops = tf.load_op_library(queue_library)
except Exception as e:
    flink_ops = tf.load_op_library(queue_library)
print("load libflink_ops.so success")


class FlinkTFRecordWriter(object):
    def __init__(self, address, container='', shared_name='', name='FlinkTFRecordWriter'):
        self.writer = flink_ops.flink_record_writer(address=address, container=container,
                                                    shared_name=shared_name, name=name)

    def write(self, tensor_list):
        res = flink_ops.flink_record_write(writer_handle=self.writer, values=tensor_list)
        return res

    def close(self):
        res = flink_ops.flink_record_close(writer_handle=self.writer)
        return res


class FlinkStreamDataSet(tf.compat.v1.data.TFRecordDataset):
    def __init__(self, filenames, compression_type=None, buffer_size=None, num_parallel_reads=None):
        super(FlinkStreamDataSet, self).__init__(filenames, compression_type, buffer_size, num_parallel_reads)
        super(FlinkStreamDataSet, self).repeat(1)

    def repeat(self, count=None):
        raise Exception("repeat func can not set!")
