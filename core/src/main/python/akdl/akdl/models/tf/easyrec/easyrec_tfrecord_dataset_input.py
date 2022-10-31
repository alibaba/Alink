# -*- encoding:utf-8 -*-
# Copyright (c) Alibaba, Inc. and its affiliates.

import tensorflow as tf
from easy_rec.python.input.input import Input
from flink_ml_tensorflow2.tensorflow_context import TFContext

if tf.__version__ >= '2.0':
    tf = tf.compat.v1


class TFRecordDataSetInput(Input):

    def __init__(self,
                 data_config,
                 feature_config,
                 task_index=0,
                 task_num=1,
                 tf_context: TFContext = None):
        super(TFRecordDataSetInput, self).__init__(data_config, feature_config, None,
                                                   task_index, task_num)

        self.feature_desc = {}
        for x, t, d in zip(self._input_fields, self._input_field_types,
                           self._input_field_defaults):
            d = self.get_type_defaults(t, d)
            t = self.get_tf_type(t)
            self.feature_desc[x] = tf.FixedLenFeature(
                dtype=t, shape=1, default_value=d)
        self.tf_context = tf_context

    def _parse_tfrecord(self, example):
        try:
            inputs = tf.parse_single_example(example, features=self.feature_desc)
        except AttributeError:
            inputs = tf.io.parse_single_example(example, features=self.feature_desc)
        return inputs

    def _build(self, mode, params):
        # file_paths = tf.gfile.Glob(self._input_path)
        # assert len(file_paths) > 0, 'match no files with %s' % self._input_path

        num_parallel_calls = self._data_config.num_parallel_calls
        if mode == tf.estimator.ModeKeys.TRAIN:
            # logging.info('train files[%d]: %s' %
            #              (len(file_paths), ','.join(file_paths)))
            # dataset = tf.data.Dataset.from_tensor_slices(file_paths)
            # if self._data_config.shuffle:
            #     # shuffle input files
            #     dataset = dataset.shuffle(len(file_paths))
            # # too many readers read the same file will cause performance issues
            # # as the same data will be read multiple times
            # parallel_num = min(num_parallel_calls, len(file_paths))
            # dataset = dataset.interleave(
            #     tf.data.TFRecordDataset,
            #     cycle_length=parallel_num,
            #     num_parallel_calls=parallel_num)
            # dataset = dataset.shard(self._task_num, self._task_index)
            dataset = self.tf_context.flink_stream_dataset()
            if self._data_config.shuffle:
                dataset = dataset.shuffle(
                    self._data_config.shuffle_buffer_size,
                    seed=2020,
                    reshuffle_each_iteration=True)
            dataset = dataset.repeat(self.num_epochs)
        else:
            # logging.info('eval files[%d]: %s' %
            #              (len(file_paths), ','.join(file_paths)))
            # dataset = tf.data.TFRecordDataset(file_paths)
            # dataset = dataset.repeat(1)
            dataset = self.tf_context.flink_stream_dataset().take(1)

        dataset = dataset.map(
            self._parse_tfrecord, num_parallel_calls=num_parallel_calls)
        dataset = dataset.batch(self._data_config.batch_size)
        dataset = dataset.prefetch(buffer_size=self._prefetch_size)
        dataset = dataset.map(
            map_func=self._preprocess, num_parallel_calls=num_parallel_calls)

        dataset = dataset.prefetch(buffer_size=self._prefetch_size)

        if mode != tf.estimator.ModeKeys.PREDICT:
            dataset = dataset.map(lambda x:
                                  (self._get_features(x), self._get_labels(x)))
        else:
            dataset = dataset.map(lambda x: (self._get_features(x)))
        return dataset
