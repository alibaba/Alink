import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTextSourceStreamOp(unittest.TestCase):
    def test_textsourcestreamop(self):

        URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv"
        data = TextSourceStreamOp().setFilePath(URL).setTextCol("text")
        data.print()
        StreamOperator.execute()
        pass