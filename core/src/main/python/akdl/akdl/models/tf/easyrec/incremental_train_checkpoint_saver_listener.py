import logging
import os
import shutil
import time

import tensorflow as tf
from akdl.runner.output_writer import OutputWriter
from tensorflow.estimator import Estimator, SessionRunHook

from oss_storage import OssStorage
from .easyrec_incremental_train_hooks import output_stream_model_to_flink


class IncrementalTrainSessionRunHook(SessionRunHook):
    estimator: Estimator
    oss_storage: OssStorage
    local_export_dir: str
    remote_base_model_dir: str
    remote_incremental_model_dir: str
    last_upload_time: int = 0
    current_model_id = None
    save_checkpoint_secs = 30
    output_writer_op: OutputWriter

    def __init__(self, estimator, oss_storage, serving_input_receiver_fn,
                 local_export_dir, local_base_model_dir, remote_base_model_dir, remote_incremental_model_dir,
                 output_writer_op) -> None:
        self.estimator = estimator
        self.oss_storage = oss_storage
        self.local_export_dir = local_export_dir
        self.input_serving_fn = serving_input_receiver_fn
        self.local_base_model_dir = local_base_model_dir
        self.remote_base_model_dir = remote_base_model_dir
        self.remote_incremental_model_dir = remote_incremental_model_dir
        self.output_writer_op = output_writer_op

    def after_run(self, run_context, run_values):
        session = run_context.session

        current_time = time.time()
        if current_time - self.last_upload_time <= self.save_checkpoint_secs:
            return

        need_update = False
        need_rebase = False
        save_model_stream = True
        latest_obj_name: str = self.oss_storage.find_latest_obj_name(self.remote_base_model_dir,
                                                                     lambda d: d.split("/")[-1].startswith("_ready_"))
        tf.logging.info("latest_obj_name = {}".format(latest_obj_name))
        if latest_obj_name is not None and latest_obj_name != self.current_model_id:
            self.current_model_id = latest_obj_name
            need_rebase = True

        if need_update:
            shutil.rmtree(self.local_export_dir, ignore_errors=True)
            self.estimator.export_saved_model(self.local_export_dir, self.input_serving_fn)
            saved_model_dir = self.local_export_dir

            ts_dir = os.listdir(saved_model_dir)[0]
            logging.info("saved_model ts_dir is {}".format(ts_dir))
            with open(os.path.join(saved_model_dir, "_ready_" + ts_dir), "w"):
                pass
            logging.info("Uploading saved model directory to oss {}".format(self.remote_incremental_model_dir))
            self.oss_storage.upload_dir(self.remote_incremental_model_dir, saved_model_dir)

            self.last_upload_time = time.time()

        if save_model_stream:
            shutil.rmtree(self.local_export_dir, ignore_errors=True)
            self.estimator.export_saved_model(self.local_export_dir, self.input_serving_fn)

            model_dir = self.local_export_dir
            savedmodel_dir = model_dir
            for d in os.listdir(model_dir):
                savedmodel_dir = os.path.join(model_dir, d)
                if os.path.isdir(savedmodel_dir):
                    break

            # f = zipfile.ZipFile("./TarName.zip",'w')    #c创建一个zip对象
            # floder = os.path.abspath(self.local_export_dir)
            # for floderName, subFolders, fileNames in os.walk(floder):
            #     f.write(floderName)
            #     for subFolder in subFolders:
            #         f.write(os.path.join(floderName,subFolder))
            #     for fileName in fileNames:
            #         f.write(os.path.join(floderName,fileName))
            # f.close()

            shutil.make_archive(base_name=savedmodel_dir, format='zip', root_dir=savedmodel_dir)
            zipfile = savedmodel_dir + ".zip"
            # zipfilename = os.path.basename(zipfile)
            output_stream_model_to_flink(zipfile, self.output_writer_op)
            self.last_upload_time = time.time()

        if need_rebase:
            shutil.rmtree(self.local_base_model_dir, ignore_errors=True)
            os.makedirs(self.local_base_model_dir, exist_ok=True)
            latest_model_prefix = os.path.relpath(self.current_model_id, self.remote_base_model_dir).lstrip("_ready_")
            tf.logging.info("latest_model_prefix = {}".format(latest_model_prefix))
            self.oss_storage.download_by_prefix(self.remote_base_model_dir, latest_model_prefix,
                                                self.local_base_model_dir)
            session.graph._unsafe_unfinalize()
            tf.train.Saver().restore(session, self.local_base_model_dir + "/" + latest_model_prefix)
            session.graph.finalize()
