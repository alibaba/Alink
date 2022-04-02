import unittest

from pyalink.alink import *


class TestMlpc(unittest.TestCase):

    def test_mlpc(self):
        URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv"
        SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
        data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

        mlpc = MultilayerPerceptronTrainBatchOp() \
            .setFeatureCols(["sepal_length", "sepal_width", "petal_length", "petal_width"]) \
            .setLabelCol("category") \
            .setLayers([4, 5, 3]) \
            .setMaxIter(20)

        predictor = MultilayerPerceptronPredictBatchOp() \
            .setPredictionCol("pred_label") \
            .setPredictionDetailCol("pred_detail")

        model = mlpc.linkFrom(data)
        predictor.linkFrom(model, data).firstN(4).print()

    def test_train(self):
        URL = "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv"
        SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string";
        data = CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR)

        mlpc = MultilayerPerceptronTrainBatchOp() \
            .setFeatureCols(["sepal_length", "sepal_width", "petal_length", "petal_width"]) \
            .setLabelCol("category") \
            .setLayers([4, 5, 3]) \
            .setMaxIter(20)

        predictor = MultilayerPerceptronPredictBatchOp() \
            .setPredictionCol("pred_label") \
            .setPredictionDetailCol("pred_detail")

        model = mlpc.linkFrom(data)
        model.print()
