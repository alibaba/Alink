import unittest

from pyalink.alink import *


class TestSpecialModels(unittest.TestCase):

    def test_run(self):
        URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv";
        SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
        data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

        lr = LogisticRegression() \
            .setFeatureCols(["sepal_length", "sepal_width", "petal_length", "petal_width"]) \
            .setLabelCol("category") \
            .setMaxIter(100) \
            .setPredictionCol("pred_result") \
            .setPredictionDetailCol("pred_detail")

        oneVsRest = OneVsRest().setClassifier(lr).setNumClass(3)
        model = oneVsRest.fit(data)
        model.transform(data).print()
