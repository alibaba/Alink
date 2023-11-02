import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestCsvSourceBatchOp(unittest.TestCase):
    def test_csvsourcebatchop(self):

        filePath = 'https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv'
        schema = 'sepal_length double, sepal_width double, petal_length double, petal_width double, category string'
        csvSource = CsvSourceBatchOp()\
            .setFilePath(filePath)\
            .setSchemaStr(schema)\
            .setFieldDelimiter(",")
        BatchOperator.collectToDataframe(csvSource)
        pass