import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTFRecordDatasetSinkStreamOp(unittest.TestCase):
    def test_tfrecorddatasetsinkstreamop(self):

        schemaStr = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string"
        source = CsvSourceStreamOp() \
            .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.csv") \
            .setSchemaStr(schemaStr)
        sink = TFRecordDatasetSinkStreamOp() \
            .setFilePath("/tmp/iris.tfrecord") \
            .setOverwriteSink(True) \
            .linkFrom(source)
        StreamOperator.execute()
        pass