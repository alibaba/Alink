import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestSelect(unittest.TestCase):
    def test_select(self):

        URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv"
        SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
        data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)
        select = Select().setClause("category as label")
        select.transform(data).print()
        pass