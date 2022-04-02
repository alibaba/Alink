import unittest

import pytest

from pyalink.alink import *


def print_value_and_type(v):
    print(type(v), v)


class TestAkBatch(unittest.TestCase):

    def setUp(self) -> None:
        self.lfs = LocalFileSystem()
        self.hfs = HadoopFileSystem("2.8.3", "hdfs://xxx:9000")
        self.ofs = OssFileSystem("3.4.1", "xxx", "xxx", "xxx", "xxx")

    @pytest.mark.skip()
    def test_batch(self):
        import numpy as np
        import pandas as pd
        arr = np.array([
            [1, 2, 3],
            [1, 2, 3],
            [3, 4, 5]
        ])
        df = pd.DataFrame(arr)
        source = BatchOperator.fromDataframe(df, "uid int, iid int, label int")

        for fs in [self.lfs, self.hfs, self.ofs]:
            if fs == self.lfs:
                filepath = FilePath("/tmp/test_alink_file_sink_str", fs)
            else:
                filepath = FilePath("tmp/test_alink_file_sink_str", fs)
            AkSinkBatchOp() \
                .setFilePath(filepath) \
                .setOverwriteSink(True) \
                .setNumFiles(3) \
                .linkFrom(source)
            BatchOperator.execute()

            AkSourceBatchOp() \
                .setFilePath(filepath) \
                .print()
