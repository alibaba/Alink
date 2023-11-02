import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestCsvSinkStreamOp(unittest.TestCase):
    def test_csvsinkstreamop(self):

        filePath = 'https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv'
        schema = 'sepal_length double, sepal_width double, petal_length double, petal_width double, category string'
        csvSource = CsvSourceStreamOp()\
            .setFilePath(filePath)\
            .setSchemaStr(schema)\
            .setFieldDelimiter(",")
        csvSink = CsvSinkStreamOp()\
            .setFilePath('~/csv_test_s.txt')
        
        csvSource.link(csvSink)
        
        StreamOperator.execute()
        pass