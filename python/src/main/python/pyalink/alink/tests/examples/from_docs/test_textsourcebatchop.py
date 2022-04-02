import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTextSourceBatchOp(unittest.TestCase):
    def test_textsourcebatchop(self):

        URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv"
        data = TextSourceBatchOp().setFilePath(URL).setTextCol("text")
        data.print()
        pass