import tempfile
import unittest

from pyalink.alink import *


class TestSpecialOperators(unittest.TestCase):

    def test_online_learning_stream_op_constructor(self):
        model = Binarizer().setSelectedCol("petal_width").setThreshold(1.)
        pipeline_model = PipelineModel(model)
        op = OnlineLearningStreamOp(pipeline_model)

    def test_pipeline_predict_stream_op_constructor(self):
        model_filename = tempfile.NamedTemporaryFile().name
        model = Binarizer().setSelectedCol("petal_width").setThreshold(1.)
        pipeline_model = PipelineModel(model)
        pipeline_model.save(model_filename)
        BatchOperator.execute()

        op = PipelinePredictStreamOp(pipeline_model).setNumThreads(1)
        self.assertEqual(type(op), PipelinePredictStreamOp)
        op = PipelinePredictStreamOp(model_filename).setNumThreads(4)
        self.assertEqual(type(op), PipelinePredictStreamOp)

    def test_pipeline_predict_stream_op_link_from(self):
        model_filename = tempfile.NamedTemporaryFile().name
        open(model_filename, "w").close()
        batch_source = CsvSourceBatchOp() \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv") \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string")
        stream_source = CsvSourceStreamOp() \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv") \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string")

        stage1 = QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["sepal_length"])
        stage2 = Binarizer().setSelectedCol("petal_width").setThreshold(1.)
        model_data = CsvSourceBatchOp().setFilePath(model_filename).setSchemaStr("model_id BIGINT, model_info STRING")
        stage3 = QuantileDiscretizerModel().setSelectedCols(["petal_length"]).setModelData(model_data)

        pipeline = Pipeline(stage1, stage3, stage2)
        pipeline_model = pipeline.fit(batch_source)

        op = PipelinePredictStreamOp(pipeline_model).setNumThreads(1).linkFrom(stream_source)
        print(op.getSchemaStr())
