import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTFRecordDatasetSinkBatchOp(unittest.TestCase):
    def test_tfrecorddatasetsinkbatchop(self):

        schemaStr = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string"
        source = CsvSourceBatchOp() \
            .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv") \
            .setSchemaStr(schemaStr)
        sink = TFRecordDatasetSinkBatchOp() \
            .setFilePath("/tmp/iris.tfrecord") \
            .setOverwriteSink(True) \
            .linkFrom(source)
        BatchOperator.execute()
        pass