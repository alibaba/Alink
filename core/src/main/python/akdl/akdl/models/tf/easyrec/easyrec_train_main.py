import os

import tensorflow as tf
# noinspection PyUnresolvedReferences
import tensorflow_io as tfio
from akdl.runner.config import TrainTaskConfig
# noinspection PyProtectedMember
from easy_rec.python.main import _train_and_evaluate_impl, export
from easy_rec.python.protos import pipeline_pb2
from easy_rec.python.utils import config_util
from google.protobuf import text_format
# noinspection PyUnresolvedReferences
from tensorflow_io.core.python.ops import core_ops


def copy_gfile_dir_to_local(remote_dir, local_dir):
    os.makedirs(local_dir, exist_ok=True)
    files = tf.io.gfile.listdir(remote_dir)
    for file in files:
        if len(file) == 0:
            continue
        if file.startswith("/"):
            file = file[1:]
        if tf.io.gfile.isdir(remote_dir + "/" + file):
            copy_gfile_dir_to_local(remote_dir + "/" + file, local_dir + "/" + file)
        else:
            tf.io.gfile.copy(remote_dir + "/" + file, local_dir + "/" + file, overwrite=True)


def remove_checkpoint_files(model_dir):
    def force_remove(path):
        if tf.io.gfile.exists(path):
            tf.io.gfile.remove(path)

    def remove_files_by_pattern(pattern):
        for f in tf.io.gfile.glob(pattern):
            tf.io.gfile.remove(f)

    def force_remove_dir(path):
        if tf.io.gfile.exists(path):
            tf.io.gfile.rmtree(path)

    if not (tf.io.gfile.exists(model_dir) and tf.io.gfile.isdir(model_dir)):
        return

    if not model_dir.endswith("/"):
        model_dir = model_dir + "/"
    force_remove(model_dir + "checkpoint")
    force_remove(model_dir + "pipeline.config")
    force_remove(model_dir + "version")
    force_remove(model_dir + "graph.pbtxt")
    remove_files_by_pattern(model_dir + "model.ckpt-*")
    remove_files_by_pattern(model_dir + "atexit_sync_*")
    remove_files_by_pattern(model_dir + "events.out.tfevents.*")
    force_remove_dir(model_dir + "eval_val")
    force_remove_dir(model_dir + "export")
    force_remove_dir(model_dir + "best_ckpt")


def main(args: TrainTaskConfig):
    user_params = args.user_params
    work_dir = args.work_dir
    task_type = args.task_type
    task_index = args.task_index

    saved_model_dir = args.saved_model_dir
    dataset_file = args.dataset_file

    pipeline_config_path = user_params["ALINK:bc_1"]
    pipeline_config = pipeline_pb2.EasyRecConfig()
    with tf.io.gfile.GFile(pipeline_config_path, 'r') as f:
        config_str = f.read()
        text_format.Merge(config_str, pipeline_config)

    model_dir = pipeline_config.model_dir
    if 'ALINK:remove_checkpoint_before_training' in user_params:
        if task_type == 'chief' and task_index == 0:
            remove_checkpoint_files(model_dir)

    exporter_type = pipeline_config.export_config.exporter_type

    config_json = {
        "train_input_path": dataset_file,
        "eval_input_path": dataset_file
    }

    config_util.edit_config(pipeline_config, config_json)
    config_util.auto_expand_share_feature_configs(pipeline_config)
    _train_and_evaluate_impl(pipeline_config, continue_train=True)

    # This is a bug when using OSS path as `model_dir` in `Estimator`. In `Estimator`, `EventFileWriter` is
    # created to write summary into `model_dir`, but estimator does not call their `close` function. For OSS,
    # the files will not show in this situation.
    # As a walk-around, we manually close `EventFileWriter` by calling `FileWriterCache.clear()`.
    tf.compat.v1.summary.FileWriterCache.clear()

    if task_type == 'chief' and task_index == 0:
        model_dir = pipeline_config.model_dir
        best_model_dir = model_dir + "/" if not model_dir.endswith("/") else model_dir
        best_model_dir += "export/" + exporter_type
        tf.compat.v1.logging.info("remote_export_dir = {}".format(best_model_dir))
        tf.compat.v1.logging.info("saved_model_dir = {}".format(saved_model_dir))

        if not tf.io.gfile.exists(best_model_dir):
            tf.compat.v1.logging.info("export model manually when #PS worker = 0")
            export(best_model_dir, pipeline_config)

        copy_gfile_dir_to_local(best_model_dir, saved_model_dir)

        latest_ckpt_dir = args.latest_ckpt_dir
        if latest_ckpt_dir is not None:
            os.makedirs(latest_ckpt_dir, exist_ok=True)
            latest = tf.train.latest_checkpoint(model_dir)
            tf.compat.v1.logging.info("latest = {}".format(latest))
            ckpt_files = [f.replace(model_dir + '/', '') for f in tf.io.gfile.glob(latest + '*')]
            for file in ['pipeline.config', 'version', 'checkpoint', 'graph.pbtxt'] + ckpt_files:
                tf.compat.v1.logging.info('copying ckpt file: {}', file)
                tf.io.gfile.copy(model_dir + "/" + file, latest_ckpt_dir + "/" + file, overwrite=True)
