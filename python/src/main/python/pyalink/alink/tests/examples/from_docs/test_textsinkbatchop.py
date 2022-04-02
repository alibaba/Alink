import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTextSinkBatchOp(unittest.TestCase):
    def test_textsinkbatchop(self):

        URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv"
        SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string"
        
        data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR).select("category")
        
        sink = TextSinkBatchOp().setFilePath('/tmp/text.csv').setOverwriteSink(True)
        data.link(sink)
        BatchOperator.execute()
        pass