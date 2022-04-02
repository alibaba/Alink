import unittest

from pyalink.alink import *


class TestTransformer(unittest.TestCase):

    def test_transformer(self):
        source = CsvSourceBatchOp().setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string").setFilePath(
            "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        assembler = VectorAssembler().setSelectedCols(["sepal_length", "sepal_width"]).setReservedCols(
            ["category"]).setOutputCol("vec")
        assembler_result = assembler.transform(source)
        assembler_result.getColNames()
        kmeans = KMeans().setVectorCol("vec").setPredictionCol("pred").setPredictionDetailCol("distance").setK(3)
        kmeans_model = kmeans.fit(assembler_result)

        pipeline = Pipeline().add(assembler).add(kmeans)
        model = pipeline.fit(source)
        model.save().collectToDataframe()
        result = model.transform(source)
        result.firstN(10).collectToDataframe()

        stream_source = CsvSourceStreamOp().setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string").setFilePath(
            "https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        stream_result = model.transform(stream_source)
        stream_result.print(key="stream_result")
        StreamOperator.execute()
