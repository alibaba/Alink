import base64
import io
import json
import logging
import os
import shutil
import struct

import tensorflow as tf
from flink_ml_framework.java_file import JavaFile

if tf.__version__ >= '2.0':
    tf = tf.compat.v1


def convert_java_queue_file_to_repeatable_dataset(java_file: JavaFile, output_data_path: str):
    data_file = io.open(output_data_path, "wb")
    cnt = 0
    while True:
        try:
            bytes_length = java_file.read(8)
            length, = struct.unpack("<Q", bytes_length)
            data_file.write(bytes_length)
            bytes_crc_masked_len = java_file.read(4)
            data_file.write(bytes_crc_masked_len)
            record_content = java_file.read(length)
            data_file.write(record_content)
            bytes_crc_masked_len = java_file.read(4)
            data_file.write(bytes_crc_masked_len)
            cnt = cnt + 1
        except Exception:
            logging.info("reached end of file")
            data_file.close()
            break
    return tf.data.TFRecordDataset(output_data_path), cnt


def find_timestamp_dir(saved_model_dir):
    timestamp_dir = saved_model_dir
    for d in os.listdir(saved_model_dir):
        timestamp_dir = os.path.join(saved_model_dir, d)
        if os.path.isdir(timestamp_dir):
            break
    return timestamp_dir


def pack_dir_to_file(directory: str, chunk_size: int):
    """
    directory should be a
    :param directory:
    :return:
    """
    shutil.make_archive(base_name=directory, format='zip', root_dir=directory)
    zip_file = directory + ".zip"
    zip_size = os.path.getsize(zip_file)
    chunks = (zip_size - 1) // chunk_size + 1
    return {
        'name': zip_file,
        'size': zip_size,
        'chunk_size': chunk_size,
        'chunks': chunks,
    }


def output_model_to_flink(model_dir, output_writer):
    """Pack the model directory to a zip file, then output the bytes of the zip file
    to Flink.
    """
    savedmodel_dir = model_dir
    for d in os.listdir(model_dir):
        savedmodel_dir = os.path.join(model_dir, d)
        if os.path.isdir(savedmodel_dir):
            break
    shutil.make_archive(base_name=savedmodel_dir, format='zip', root_dir=savedmodel_dir)
    zipfile = savedmodel_dir + ".zip"

    chunk_size = 1024 * 1024
    zip_filename_encoded = os.path.basename(zipfile).encode("utf8")

    example = tf.train.Example(features=tf.train.Features(
        feature={
            'model_id': tf.train.Feature(int64_list=tf.train.Int64List(value=[0])),
            'model_info': tf.train.Feature(
                bytes_list=tf.train.BytesList(value=[zip_filename_encoded])),
        }))
    output_writer.write(example)

    with open(zipfile, 'rb') as f:
        chunk_id = 1
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            chunk = base64.b64encode(chunk)
            example = tf.train.Example(features=tf.train.Features(
                feature={
                    'model_id': tf.train.Feature(int64_list=tf.train.Int64List(value=[chunk_id])),
                    'model_info': tf.train.Feature(bytes_list=tf.train.BytesList(value=[chunk])),
                }))
            chunk_id = chunk_id + 1
            output_writer.write(example)


def output_model_ckpt_to_flink(saved_model_dir, latest_ckpt_dir, output_writer):
    """
    Pack saved model directory and latest checkpoint dir to zip files, then output the bytes of the zip file to Flink.
    """
    chunk_size = 1024 * 1024
    file_summaries = []
    timestamp_dir = find_timestamp_dir(saved_model_dir)
    file_summaries.append(pack_dir_to_file(timestamp_dir, chunk_size))
    file_summaries.append(pack_dir_to_file(latest_ckpt_dir, chunk_size))

    example = tf.train.Example(features=tf.train.Features(
        feature={
            'model_id': tf.train.Feature(int64_list=tf.train.Int64List(value=[0])),
            'model_info': tf.train.Feature(
                bytes_list=tf.train.BytesList(value=[json.dumps(file_summaries).encode('utf8')])),
        }))
    output_writer.write(example)

    chunk_id = 0
    for file_summary in file_summaries:
        name = file_summary['name']
        with open(name, 'rb') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                chunk = base64.b64encode(chunk)
                chunk_id += 1
                example = tf.train.Example(features=tf.train.Features(
                    feature={
                        'model_id': tf.train.Feature(int64_list=tf.train.Int64List(value=[chunk_id])),
                        'model_info': tf.train.Feature(bytes_list=tf.train.BytesList(value=[chunk])),
                    }))
                output_writer.write(example)


def unpack_model_from_flink(model_path, work_dir):
    """ Unpack the model, which is written to local disk by Flink's task """
    bc_model_fn = model_path
    lines = {}
    with open(bc_model_fn, 'r') as f:
        while True:
            line = f.readline()
            if not line:
                break
            space_pos = line.index(' ')
            id = int(line[0:space_pos])
            lines[id] = line[space_pos + 1:]

    zip_file_name = lines[0].strip()
    print('zip file: ', zip_file_name)
    zip_file_name = os.path.join(work_dir, zip_file_name)

    with open(zip_file_name, 'wb') as f:
        for id, value in sorted(lines.items()):
            if id == 0:
                continue
            f.write(base64.b64decode(s=value, validate=True))

    import zipfile
    zip_ref = zipfile.ZipFile(zip_file_name, 'r')
    unzip_dir = zip_file_name[0:-len('.zip')]
    zip_ref.extractall(path=unzip_dir)
    zip_ref.close()
    return unzip_dir


def remove_checkpoint_files(model_dir):
    def force_remove(path):
        if tf.gfile.Exists(path):
            tf.gfile.Remove(path)

    def remove_files_by_pattern(pattern):
        for f in tf.gfile.Glob(pattern):
            tf.gfile.Remove(f)

    def force_remove_dir(path):
        if tf.gfile.Exists(path):
            tf.gfile.DeleteRecursively(path)

    if not (tf.gfile.Exists(model_dir) and tf.gfile.IsDirectory(model_dir)):
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
    force_remove_dir(model_dir + "keras")
    force_remove_dir(model_dir + "eval_val")
    force_remove_dir(model_dir + "export")
    force_remove_dir(model_dir + "best_ckpt")
    force_remove_dir(model_dir + "pretrained_ckpt") # for EasyTransfer


def gfile_copy_dir_content(remote_dir, local_dir):
    logging.info("copy from {} to {}".format(remote_dir, local_dir))
    tf.gfile.MakeDirs(local_dir)
    files = tf.gfile.ListDirectory(remote_dir)
    for file in files:
        if len(file) == 0:
            continue
        if file.startswith("/"):
            file = file[1:]
        if tf.gfile.IsDirectory(remote_dir + "/" + file):
            gfile_copy_dir_content(remote_dir + "/" + file, local_dir + "/" + file)
        else:
            file_io_copy_fix(remote_dir + "/" + file, local_dir + "/" + file, True)


def file_io_copy_fix(src, dst, overwrite=False):
    if (not overwrite) and tf.gfile.Exists(dst):
        raise ValueError("dst file {} exists.".format(dst))
    with tf.gfile.GFile(src, "rb") as fin, tf.gfile.GFile(dst, "wb") as fout:
        shutil.copyfileobj(fin, fout)


if tf.__version__ >= '2.0':
    pass
else:
    tf.python.file_io.copy_v2 = file_io_copy_fix  # replace copy for OSS
