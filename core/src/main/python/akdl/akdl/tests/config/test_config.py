import unittest

import tensorflow as tf

if tf.__version__ >= '2.0':
    tf = tf.compat.v1

from akdl.runner.config import BatchTaskConfig, StreamTaskConfig, TrainTaskConfig


def print_dataset(dataset: tf.data.Dataset):
    next_record = dataset.make_one_shot_iterator().get_next()
    counter = 0
    with tf.Session() as sess:
        while True:
            try:
                record = sess.run(next_record)
                example = tf.train.Example.FromString(record)
                if counter < 10:
                    print(example)
                counter += 1
            except tf.errors.OutOfRangeError:
                break
    print("total examples: " + str(counter))


def batch_main(args: BatchTaskConfig):
    print_dataset(args.dataset)


def stream_main(args: StreamTaskConfig):
    print_dataset(args.dataset_fn())


def train_main(args: TrainTaskConfig):
    print_dataset(args.dataset)


class TestConfig(unittest.TestCase):
    def test_batch_task_config(self):
        tfrecords_path = "dataset.tfrecords"
        batch_main(BatchTaskConfig(
            tf_context=None,
            cluster=None,
            dataset_length=None,
            dataset=tf.data.TFRecordDataset(tfrecords_path),
            task_type='chief',
            task_index=0,
            num_workers=1,
            work_dir=None,
            dataset_file=tfrecords_path,
            user_params={},
            output_writer=None
        ))

    def test_stream_task_config(self):
        tfrecords_path = "dataset.tfrecords"
        stream_main(StreamTaskConfig(
            tf_context=None,
            cluster=None,
            dataset_length=None,
            dataset_fn=lambda: tf.data.TFRecordDataset(tfrecords_path),
            task_type='chief',
            task_index=0,
            num_workers=1,
            work_dir=None,
            user_params={},
            output_writer=None
        ))

    def test_train_task_config(self):
        tfrecords_path = "dataset.tfrecords"
        train_main(TrainTaskConfig(
            tf_context=None,
            cluster=None,
            dataset_length=None,
            dataset=tf.data.TFRecordDataset(tfrecords_path),
            task_type='chief',
            task_index=0,
            num_workers=1,
            work_dir=None,
            dataset_file=tfrecords_path,
            user_params={},
            saved_model_dir=None
        ))
