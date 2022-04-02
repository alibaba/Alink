import unittest

from pyalink.alink import *


class TestStream(unittest.TestCase):

    def test_stream(self):
        source = CsvSourceStreamOp() \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv") \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string")
        source.print(key="source", refreshInterval=3)
        StreamOperator.execute()
