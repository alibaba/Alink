import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTFRecordDatasetSourceStreamOp(unittest.TestCase):
    def test_tfrecorddatasetsourcestreamop(self):

        schemaStr = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string"
        source = TFRecordDatasetSourceStreamOp() \
            .setFilePath("https://alink-release.oss-cn-beijing.aliyuncs.com/data-files/iris.tfrecord") \
            .setSchemaStr(schemaStr)
        source.print()
        StreamOperator.execute()
        pass