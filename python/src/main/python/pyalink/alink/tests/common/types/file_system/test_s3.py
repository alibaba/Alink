import unittest

import pandas as pd
import pytest

from pyalink.alink import *


class TestS3FileSystem(unittest.TestCase):

    @pytest.mark.skip()
    def test_hadoop_s3(self):
        data = [
            ["0L", "1L", 0.6],
            ["2L", "2L", 0.8],
            ["2L", "4L", 0.6],
            ["3L", "1L", 0.6],
            ["3L", "2L", 0.3],
            ["3L", "4L", 0.4],
        ]

        source = BatchOperator.fromDataframe(pd.DataFrame.from_records(data), "uid string, iid string, label float")

        fs = S3HadoopFileSystem("1.11.788", "http://xxx:9000", "test", "xxx", "xxx", True)
        print(fs.listFiles("/"))

        source.link(
            AkSinkBatchOp()
                .setFilePath(FilePath("/tmp/test_alink_file_sink", fs))
                .setOverwriteSink(True)
                .setNumFiles(2)
        )

        BatchOperator.execute()

        AkSourceBatchOp() \
            .setFilePath(FilePath("/tmp/test_alink_file_sink", fs)) \
            .print()

    @pytest.mark.skip()
    def test_presto_s3(self):
        data = [
            ["0L", "1L", 0.6],
            ["2L", "2L", 0.8],
            ["2L", "4L", 0.6],
            ["3L", "1L", 0.6],
            ["3L", "2L", 0.3],
            ["3L", "4L", 0.4],
        ]

        source = BatchOperator.fromDataframe(pd.DataFrame.from_records(data), "uid string, iid string, label float")

        fs = S3PrestoFileSystem("1.11.788", "http://xxx:9000", "test", "xxx", "xxx", True)
        print(fs.listFiles("/"))

        source.link(
            AkSinkBatchOp()
                .setFilePath(FilePath("/tmp/test_alink_file_sink2", fs))
                .setOverwriteSink(True)
                .setNumFiles(2)
        )

        BatchOperator.execute()

        AkSourceBatchOp() \
            .setFilePath(FilePath("/tmp/test_alink_file_sink2", fs)) \
            .print()
