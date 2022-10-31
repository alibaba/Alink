import logging
import os
import tempfile
from typing import Callable

import oss2
from oss2.models import SimplifiedObjectInfo


class OssStorage:
    MAX_RETRIES = 16

    def __init__(self, access_key_id, access_key_secret, endpoint, bucket):
        self.auth = oss2.Auth(access_key_id, access_key_secret)
        self.bucket = oss2.Bucket(self.auth, endpoint, bucket, connect_timeout=30)

    def upload(self, object_name, local_file):
        """
        Upload file `local_file` to be `object_name` with resumable upload and retries
        :param object_name: oss object name
        :param local_file: local file
        :return:
        """
        logging.info("Upload file {} to {}".format(local_file, object_name))
        retry_count = 0
        while True:
            try:
                retry_count += 1
                oss2.resumable_upload(self.bucket, object_name, local_file)
                break
            except:
                if retry_count >= self.MAX_RETRIES:
                    raise

    def upload_dir(self, remote_dir, local_dir):
        """
        Upload directory `local_dir` to be inside `remote_dir` with resumable upload and retries
        :param remote_dir: remote_dir
        :param local_dir: local file
        :return:
        """
        logging.info("Upload dir {} to {}".format(local_dir, remote_dir))
        for dirpath, _, filenames in os.walk(local_dir):
            relative_dir = os.path.relpath(dirpath, local_dir)
            for filename in filenames:
                remote_path = os.path.join(remote_dir, relative_dir, filename) if relative_dir != "." else os.path.join(
                    remote_dir, filename)
                local_path = os.path.join(dirpath, filename)
                self.upload(remote_path, local_path)

    def download(self, object_name, local_file):
        """
        Download file `object_name` to be `local_file` with resumable download and retries
        :param object_name: oss object name
        :param local_file: local file
        :return:
        """
        os.makedirs(os.path.dirname(local_file), exist_ok=True)
        retry_count = 0
        while True:
            try:
                retry_count += 1
                oss2.resumable_download(self.bucket, object_name, local_file)
                break
            except:
                if retry_count >= self.MAX_RETRIES:
                    raise

    def download_by_prefix(self, remote_dir, prefix, local_dir):
        """
        Download multiple files in `remote_dir` with prefix `prefix` to be inside `local_dir` with resumable download
        and retries
        :param remote_dir: remote directory
        :param prefix: prefix
        :param local_dir: local directory
        :return:
        """
        logging.info("Downloading from {} with prefix {} to {}".format(remote_dir, prefix, local_dir))
        for obj in oss2.ObjectIterator(self.bucket, os.path.join(remote_dir, prefix)):
            obj: SimplifiedObjectInfo = obj
            relative_path = os.path.relpath(obj.key, remote_dir)
            local_file = os.path.join(local_dir, relative_path)
            self.download(obj.key, local_file)

    def upload_empty_file(self, remote_dir: str, basename: str):
        filename = tempfile.NamedTemporaryFile().name
        with open(filename, "w"):
            pass
        self.upload(os.path.join(remote_dir, basename), filename)

    def find_latest_obj_name(self, prefix: str, predicate: Callable[[str], bool]):
        """
        Find latest object name in oss directory `prefix` satisfying `predicate`
        :param prefix: oss directory
        :param predicate: conditions to be satisfied
        :return:
        """
        if not prefix.endswith('/'):
            raise ValueError("prefix must end with '/'")
        latest_obj = None
        latest_modified_time = 0
        try:
            for obj in oss2.ObjectIterator(self.bucket, prefix, delimiter='/'):
                obj: SimplifiedObjectInfo = obj
                if not predicate(obj.key):
                    continue
                if obj.last_modified > latest_modified_time:
                    latest_obj = obj
                    latest_modified_time = obj.last_modified
        except:
            logging.info("Cannot find_latest_obj_name")
            return None
        if latest_obj is None:
            return None
        return latest_obj.key
