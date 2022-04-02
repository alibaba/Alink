import unittest

from pyalink.alink import *


class TestDistinct(unittest.TestCase):

    def test_distinct(self):
        URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv"
        SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
        data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
        data = data.select('category').link(DistinctBatchOp())
        data.print()
