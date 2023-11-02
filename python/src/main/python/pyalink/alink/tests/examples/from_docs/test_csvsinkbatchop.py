import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestCsvSinkBatchOp(unittest.TestCase):
    def test_csvsinkbatchop(self):

        filePath = 'https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv'
        schema = 'sepal_length double, sepal_width double, petal_length double, petal_width double, category string'
        csvSource = CsvSourceBatchOp()\
            .setFilePath(filePath)\
            .setSchemaStr(schema)\
            .setFieldDelimiter(",")
        csvSink = CsvSinkBatchOp()\
            .setFilePath('~/csv_test.txt')
        
        csvSource.link(csvSink)
        
        BatchOperator.execute()
        pass