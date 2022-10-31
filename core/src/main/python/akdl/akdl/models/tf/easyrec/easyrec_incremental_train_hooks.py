import base64
import logging
import os
import shutil
import tensorflow as tf
import time
from akdl.runner.output_writer import DirectOutputWriter
from tensorflow.estimator import Estimator, SessionRunHook

__all__ = ['OutputModelHook']


def output_stream_model_to_flink(model_path, writer: DirectOutputWriter, model_counter: int):
    """Pack the model directory to a zip file, then output the bytes of the zip file to Flink.
    """
    shutil.make_archive(base_name=model_path, format='zip', root_dir=model_path)
    zip_filepath = model_path + ".zip"
    zip_file_size = os.path.getsize(zip_filepath)

    chunk_size = 1024 * 1024
    num_chunks = int((zip_file_size + chunk_size - 1) / chunk_size) + 1
    zip_filename_encoded = os.path.basename(zip_filepath).encode("utf8")

    example = tf.train.Example(features=tf.train.Features(
        feature={
            'alinkmodelstreamtimestamp': tf.train.Feature(int64_list=tf.train.Int64List(value=[model_counter])),
            'alinkmodelstreamcount': tf.train.Feature(int64_list=tf.train.Int64List(value=[num_chunks])),
            'model_id': tf.train.Feature(int64_list=tf.train.Int64List(value=[0])),
            'model_info': tf.train.Feature(bytes_list=tf.train.BytesList(value=[zip_filename_encoded])),
        }))
    writer.write(example)

    with open(zip_filepath, 'rb') as f:
        chunk_id = 1
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            chunk = base64.b64encode(chunk)
            example = tf.train.Example(features=tf.train.Features(
                feature={
                    'alinkmodelstreamtimestamp': tf.train.Feature(int64_list=tf.train.Int64List(value=[model_counter])),
                    'alinkmodelstreamcount': tf.train.Feature(int64_list=tf.train.Int64List(value=[num_chunks])),
                    'model_id': tf.train.Feature(int64_list=tf.train.Int64List(value=[chunk_id])),
                    'model_info': tf.train.Feature(bytes_list=tf.train.BytesList(value=[chunk])),
                }))
            chunk_id = chunk_id + 1
            writer.write(example)


def output_model(estimator: Estimator, local_export_dir: str, input_serving_fn, writer: DirectOutputWriter,
                 model_counter: int):
    shutil.rmtree(local_export_dir, ignore_errors=True)
    estimator.export_saved_model(local_export_dir, input_serving_fn)
    tf_name = os.listdir(local_export_dir)[0]
    logging.info("saved_model tf_name is {}".format(tf_name))
    with open(os.path.join(local_export_dir, "_ready_" + tf_name), "w"):
        pass
    model_path = os.path.join(local_export_dir, tf_name)
    output_stream_model_to_flink(model_path, writer, model_counter)


class OutputModelHook(SessionRunHook):
    is_chief: bool
    writer: DirectOutputWriter
    local_export_dir: str
    last_output_time: float = 0.
    current_model_id = None
    model_counter = 0

    estimator: Estimator = None
    serving_input_receiver_fn = None

    def __init__(self, is_chief: bool, writer: DirectOutputWriter, local_export_dir: str, output_model_secs=30) -> None:
        self.is_chief = is_chief
        self.writer = writer
        self.local_export_dir = local_export_dir
        self.output_model_secs = output_model_secs

    def set_estimator_and_serving_fn(self, estimator, serving_input_receiver_fn):
        self.estimator = estimator
        self.serving_input_receiver_fn = serving_input_receiver_fn

    def set_output_model_secs(self, output_model_secs):
        self.output_model_secs = output_model_secs

    def need_output_model(self) -> bool:
        current_time = time.time()
        if current_time - self.last_output_time <= self.output_model_secs:
            return False
        tf.compat.v1.logging.info("last_output_time = {}, current_time = {}, output_model_secs = {}"
                                  .format(self.last_output_time, current_time, self.output_model_secs))
        return True

    def after_run(self, run_context, run_values):
        if not self.is_chief:
            return
        if not self.need_output_model():
            return
        self.model_counter += 1
        print(f'Here~ {self.serving_input_receiver_fn}', flush=True)
        output_model(self.estimator, self.local_export_dir,
                     self.serving_input_receiver_fn, self.writer, self.model_counter)
        self.last_output_time = time.time()
