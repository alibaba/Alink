import json
import logging
import os
from shutil import copyfileobj
from time import sleep
from typing import Dict

from akdl.runner.config import TrainTaskConfig
import tensorflow as tf
from akdl.runner.io_helper import remove_checkpoint_files

if tf.__version__ >= '2.0':
    tf = tf.compat.v1

# noinspection PyProtectedMember
from easytransfer import Config
from easytransfer.app_zoo import BertTextMatch, BertTextClassify
from easytransfer.datasets import TFRecordReader
# noinspection PyUnresolvedReferences
from tensorflow_core.python.data import TFRecordDataset


def gfile_copy_dir(remote_dir, local_dir):
    logging.info("copy from {} to {}".format(remote_dir, local_dir))
    tf.gfile.MakeDirs(local_dir)
    files = tf.gfile.ListDirectory(remote_dir)
    for file in files:
        if len(file) == 0:
            continue
        if file.startswith("/"):
            file = file[1:]
        if tf.gfile.IsDirectory(remote_dir + "/" + file):
            gfile_copy_dir(remote_dir + "/" + file, local_dir + "/" + file)
        else:
            file_io_copy_fix(remote_dir + "/" + file, local_dir + "/" + file, True)


def file_io_copy_fix(src, dst, overwrite=False):
    if (not overwrite) and tf.gfile.Exists(dst):
        raise ValueError("dst file {} exists.".format(dst))
    with tf.gfile.GFile(src, "rb") as fin, tf.gfile.GFile(dst, "wb") as fout:
        copyfileobj(fin, fout)


tf.python.file_io.copy_v2 = file_io_copy_fix  # replace copy for OSS


def patch_obj(obj, v, *paths):
    if len(paths) == 1:
        obj[paths[0]] = v
    else:
        if paths[0] not in obj:
            obj[paths[0]] = {}
        patch_obj(obj[paths[0]], v, *paths[1:])


def main(task_config: TrainTaskConfig):
    logging.info("TF_CONFIG = {}".format(os.environ['TF_CONFIG']))
    logging.info("args = {}".format(task_config))
    user_params = task_config.user_params
    work_dir = task_config.work_dir
    task_type = task_config.task_type
    task_index = task_config.task_index

    saved_model_dir = task_config.saved_model_dir
    dataset_file = task_config.dataset_file

    config_json = json.loads(user_params.get("config_json"))
    app_name = user_params.get("app_name")

    if 'TF_CONFIG' in os.environ:
        gpu_devices = [d for d in tf.config.experimental.list_logical_devices() if 'GPU' in d.device_type]
        info = json.loads(os.environ['TF_CONFIG'])
        workers = info['cluster'].get('ps', []) + info['cluster'].get('worker', []) + info['cluster']['chief']
        patch_obj(config_json, info['task']['type'], 'job_name')
        patch_obj(config_json, info['task']['index'], 'task_index')
        patch_obj(config_json, 0 if len(gpu_devices) == 0 else 1, 'num_gpus')
        patch_obj(config_json, ','.join(workers), 'worker_hosts')
        patch_obj(config_json, len(workers), 'num_workers')

    model_dir = user_params.get("model_dir", os.path.join(work_dir, "model_dir"))
    patch_obj(config_json, dataset_file, 'train_config', 'train_input_fp')
    patch_obj(config_json, model_dir, 'train_config', 'model_dir')
    patch_obj(config_json, saved_model_dir, 'export_config', 'export_dir_base')

    if 'ALINK:remove_checkpoint_before_training' in user_params:
        if task_type == 'chief' and task_index == 0:
            remove_checkpoint_files(model_dir)

    use_google_archives = True
    if use_google_archives and "pretrained_ckpt_path" in user_params:
        local_ckpt_dir: str = user_params.get("pretrained_ckpt_path")
        if not local_ckpt_dir.startswith("/"):
            local_ckpt_dir = os.path.join(work_dir, user_params.get("pretrained_ckpt_path"))
        # In Google Bert archives, checkpoints are prefixed with 'bert_model.ckpt', and config file is bert_config.json.
        # We convert them to what EasyTransfer require.
        bert_config: Dict[str, str] = json.load(open(os.path.join(local_ckpt_dir, 'bert_config.json'), 'r'))
        bert_config['model_type'] = 'bert'
        json.dump(bert_config, open(os.path.join(local_ckpt_dir, 'config.json'), 'w'))

        # In PS mode, we have to upload files in local pretrained_ckpt_path to the remote model_dir, so all workers
        # can access the same location.
        tf_config = json.loads(os.environ['TF_CONFIG'])
        if 'ps' in tf_config['cluster'] and not model_dir.startswith(work_dir):
            remote_ckpt_dir = model_dir + "/" + "pretrained_ckpt"
            flag_file = model_dir + "/" + "pretrained_copied"
            if task_type == 'chief' and task_index == 0:
                gfile_copy_dir(local_ckpt_dir, remote_ckpt_dir)
                with tf.gfile.Open(flag_file, "w") as f:
                    f.write("ready!")
                    f.close()
            else:
                while not tf.gfile.Exists(flag_file):
                    logging.info("Waiting")
                    sleep(1.)
        else:
            remote_ckpt_dir = local_ckpt_dir
        patch_obj(config_json, os.path.join(remote_ckpt_dir, "bert_model.ckpt"),
                  'model_config', 'pretrain_model_name_or_path')

    logging.info("config = {}".format(config_json))

    config = Config(mode="train", config_json=config_json)

    intra_op_parallelism = int(task_config.tf_context.properties['ALINK:intra_op_parallelism'])
    logging.info("intra_op_parallelism = {}".format(intra_op_parallelism))
    config.intra_op_parallelism = intra_op_parallelism

    if app_name == 'TEXT_CLASSIFY':
        app_cls = BertTextClassify
    elif app_name == 'TEXT_MATCH':
        app_cls = BertTextMatch
    app = app_cls(user_defined_config=config)

    train_reader = TFRecordReader(
        input_glob=app.train_input_fp,
        is_training=True,
        input_schema=app.input_schema,
        batch_size=app.train_batch_size
    )

    app.run_train(reader=train_reader)

    if task_type == 'chief' and task_index == 0:
        latest = tf.train.latest_checkpoint(model_dir)
        tf.logging.info("latest = {}".format(latest))
        config_json['export_config']['checkpoint_path'] = latest
        config = Config(mode="export", config_json=config_json)
        app = app_cls(user_defined_config=config)
        app.export_model()
        # model_dir = model_dir
        # best_model_dir = model_dir + "/" if not model_dir.endswith("/") else model_dir
        # best_model_dir += "export/" + exporter_type
        # tf.logging.info("remote_export_dir = {}".format(best_model_dir))
        # copy_gfile_dir_to_local(best_model_dir, saved_model_dir)
