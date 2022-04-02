import tempfile
import unittest

from pyalink.alink import *


class TestPipeline(unittest.TestCase):

    def setUp(self) -> None:
        self.source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")

        self.stream_source = CsvSourceStreamOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")

    def test_pipeline_op(self):
        stage1 = QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["sepal_length"]).setReservedCols(["petal_width"])
        stage2 = QuantileDiscretizer().setNumBuckets(3).setSelectedCols(["petal_width"]).setReservedCols([])
        pipeline = Pipeline(stage1, stage2)
        self.assertEqual(pipeline._stages, [stage1, stage2])
        removed = pipeline.remove(1)
        self.assertEqual(removed, stage2)
        self.assertEqual(pipeline._stages, [stage1])
        stage3 = Binarizer().setSelectedCol("petal_width").setThreshold(1.)
        pipeline.add(0, stage3)
        self.assertEqual(pipeline._stages, [stage3, stage1])
        self.assertEqual(pipeline.get(1), stage1)
        self.assertEqual(pipeline.size(), 2)

    def test_estimator_base(self):
        stage = QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["sepal_length"])
        model = stage.fit(self.source)
        self.assertEqual(type(model), QuantileDiscretizerModel)
        model.transform(self.source).print()
        model.transform(self.stream_source).print()
        StreamOperator.execute()

    def test_transform_base(self):
        stage = Binarizer().setSelectedCol("petal_width").setThreshold(1.)
        stage.transform(self.source).print()
        stage.transform(self.stream_source).print()
        StreamOperator.execute()

    def test_model_base(self):
        model_filename = tempfile.NamedTemporaryFile().name

        # save model data to file
        stage1 = QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["sepal_length"])
        model1 = stage1.fit(self.source)
        model_data = model1.getModelData()
        model_data.link(
            CsvSinkBatchOp().setFilePath(model_filename).setOverwriteSink(True)
        )
        BatchOperator.execute()

        # model base can only be used with setModelData
        model_data = CsvSourceBatchOp().setFilePath(model_filename).setSchemaStr("model_id BIGINT, model_info STRING")
        model2 = QuantileDiscretizerModel().setSelectedCols(["sepal_length"]).setModelData(model_data)
        model2.transform(self.source).print()
        model2.transform(self.stream_source).print()
        StreamOperator.execute()

    def test_pipeline_with_estimator_transformer_model(self):
        model_filename = tempfile.NamedTemporaryFile().name

        # save model data to file
        QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["petal_length"])\
            .fit(self.source)\
            .getModelData()\
            .link(CsvSinkBatchOp().setFilePath(model_filename).setOverwriteSink(True))
        BatchOperator.execute()

        stage1 = QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["sepal_length"])
        stage2 = Binarizer().setSelectedCol("petal_width").setThreshold(1.)
        model_data = CsvSourceBatchOp().setFilePath(model_filename).setSchemaStr("model_id BIGINT, model_info STRING")
        stage3 = QuantileDiscretizerModel().setSelectedCols(["petal_length"]).setModelData(model_data)

        pipeline = Pipeline(stage1, stage3, stage2)
        self.assertEqual(pipeline._stages, [stage1, stage3, stage2])
        pipeline_model = pipeline.fit(self.source)
        pipeline_model.transform(self.source).print()
        pipeline_model.transform(self.stream_source).print()
        StreamOperator.execute()

    def test_pipeline_model_get_transformers(self):
        model_filename = tempfile.NamedTemporaryFile().name

        # save model data to file
        QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["petal_length"])\
            .fit(self.source)\
            .getModelData()\
            .link(CsvSinkBatchOp().setFilePath(model_filename).setOverwriteSink(True))
        BatchOperator.execute()

        stage1 = QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["sepal_length"])
        stage2 = Binarizer().setSelectedCol("petal_width").setThreshold(1.)
        model_data = CsvSourceBatchOp().setFilePath(model_filename).setSchemaStr("model_id BIGINT, model_info STRING")
        stage3 = QuantileDiscretizerModel().setSelectedCols(["petal_length"]).setModelData(model_data)

        pipeline = Pipeline(stage1, stage3, stage2)
        self.assertEqual(pipeline._stages, [stage1, stage3, stage2])
        pipeline_model = pipeline.fit(self.source)
        transformers = pipeline_model.getTransformers()
        self.assertEqual(type(transformers[0]), QuantileDiscretizerModel)
        self.assertEqual(type(transformers[1]), QuantileDiscretizerModel)
        self.assertEqual(type(transformers[2]), Binarizer)

        transformers[0].getModelData().link(QuantileDiscretizerModelInfoBatchOp().lazyPrintModelInfo())
        BatchOperator.execute()

    def test_pipeline_with_pipeline(self):
        model_filename = tempfile.NamedTemporaryFile().name

        # save model data to file
        QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["petal_length"])\
            .fit(self.source)\
            .getModelData()\
            .link(CsvSinkBatchOp().setFilePath(model_filename).setOverwriteSink(True))
        BatchOperator.execute()

        # construct a pipeline
        stage1 = QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["sepal_length"])
        stage2 = Binarizer().setSelectedCol("petal_width").setThreshold(1.)
        model_data = CsvSourceBatchOp().setFilePath(model_filename).setSchemaStr("model_id BIGINT, model_info STRING")
        stage3 = QuantileDiscretizerModel().setSelectedCols(["petal_length"]).setModelData(model_data)
        pipeline1 = Pipeline(stage1, stage2, stage3)

        # construct a second pipeline
        stage4 = pipeline1
        stage5 = Binarizer().setSelectedCol("sepal_width").setThreshold(1.)
        pipeline2 = Pipeline(stage4, stage1, stage5)
        pipeline_model = pipeline2.fit(self.source)

        pipeline_model.transform(self.source).print()
        pipeline_model.transform(self.stream_source).print()
        StreamOperator.execute()

    def test_pipeline_with_pipeline_model(self):
        # save model data to file (ModelBase)
        model_filename = tempfile.NamedTemporaryFile().name
        train = QuantileDiscretizerTrainBatchOp().setNumBuckets(2).setSelectedCols(["petal_length"]) \
            .linkFrom(self.source)
            # .fit(self.source)\
            # .getModelData()\
        train.link(AkSinkBatchOp().setFilePath(model_filename).setOverwriteSink(True))
        BatchOperator.execute()

        # save pipeline model data to file
        pipeline_model_filename = tempfile.NamedTemporaryFile().name
        stage1 = QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["sepal_length"])
        stage2 = Binarizer().setSelectedCol("petal_width").setThreshold(1.)
        model_data = AkSourceBatchOp().setFilePath(model_filename)
        stage3 = QuantileDiscretizerModel().setSelectedCols(["petal_length"]).setModelData(model_data)
        prev_pipeline_model = Pipeline(stage1, stage2, stage3).fit(self.source)
        prev_pipeline_model.save(pipeline_model_filename)
        BatchOperator.execute()

        # construct a Pipeline with PipelineModel.load and test
        stage1 = PipelineModel.load(pipeline_model_filename)
        stage2 = Binarizer().setSelectedCol("sepal_width").setThreshold(1.)
        pipeline = Pipeline(stage1, stage2)
        pipeline_model = pipeline.fit(self.source)
        pipeline_model.transform(self.source).print()
        pipeline_model.transform(self.stream_source).print()
        StreamOperator.execute()

        # construct a Pipeline with an existing PipelineModel and test
        stage1 = PipelineModel.load(pipeline_model_filename)
        stage2 = Binarizer().setSelectedCol("sepal_width").setThreshold(1.)
        pipeline = Pipeline(prev_pipeline_model, stage2)
        pipeline_model = pipeline.fit(self.source)
        pipeline_model.transform(self.source).print()
        pipeline_model.transform(self.stream_source).print()
        StreamOperator.execute()

    def test_pipeline_model_constructor(self):
        # save model data to file (ModelBase)
        model_filename = tempfile.NamedTemporaryFile().name
        QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["petal_length"])\
            .fit(self.source)\
            .getModelData()\
            .link(CsvSinkBatchOp().setFilePath(model_filename).setOverwriteSink(True))
        BatchOperator.execute()

        # save pipeline model data to file
        pipeline_model_filename = tempfile.NamedTemporaryFile().name
        model1 = QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["sepal_length"]).fit(self.source)
        model2 = Binarizer().setSelectedCol("petal_width").setThreshold(1.)
        model_data = CsvSourceBatchOp().setFilePath(model_filename).setSchemaStr("model_id BIGINT, model_info STRING")
        model3 = QuantileDiscretizerModel().setSelectedCols(["petal_length"]).setModelData(model_data)

        pipeline_model = PipelineModel(model1, model2, model3)
        pipeline_model.transform(self.source).print()
        pipeline_model.transform(self.stream_source).print()
        StreamOperator.execute()

    def test_pipeline_save_load(self):
        # save model data to file (ModelBase)
        model_filename = tempfile.NamedTemporaryFile().name
        train = QuantileDiscretizerTrainBatchOp().setNumBuckets(2).setSelectedCols(["petal_length"]) \
            .linkFrom(self.source)
            # .fit(self.source)\
            # .getModelData()\
        train.link(AkSinkBatchOp().setFilePath(model_filename).setOverwriteSink(True))
        BatchOperator.execute()

        # save pipeline model data to file
        pipeline_model_filename = tempfile.NamedTemporaryFile().name
        stage1 = QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["sepal_length"])
        stage2 = Binarizer().setSelectedCol("petal_width").setThreshold(1.)
        model_data = AkSourceBatchOp().setFilePath(model_filename)
        stage3 = QuantileDiscretizerModel().setSelectedCols(["petal_length"]).setModelData(model_data)
        pipeline = Pipeline(stage1, stage2, stage3)
        pipeline2 = Pipeline.collectLoad(pipeline.save())
        pipeline_model = pipeline2.fit(self.source)
        pipeline_model.transform(self.source).print()

    def test_pipeline_model_save_load(self):
        # save model data to file (ModelBase)
        model_filename = tempfile.NamedTemporaryFile().name
        QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["petal_length"]) \
            .fit(self.source) \
            .getModelData() \
            .link(CsvSinkBatchOp().setFilePath(model_filename).setOverwriteSink(True))
        BatchOperator.execute()

        # save pipeline model data to file
        pipeline_model_filename = tempfile.NamedTemporaryFile().name
        model1 = QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["sepal_length"]).fit(self.source)
        model2 = Binarizer().setSelectedCol("petal_width").setThreshold(1.)
        model_data = CsvSourceBatchOp().setFilePath(model_filename).setSchemaStr("model_id BIGINT, model_info STRING")
        model3 = QuantileDiscretizerModel().setSelectedCols(["petal_length"]).setModelData(model_data)

        pipeline_model = PipelineModel(model1, model2, model3)
        pipeline_model = PipelineModel.collectLoad(pipeline_model.save())
        pipeline_model.transform(self.source).print()
        pipeline_model.transform(self.stream_source).print()
        StreamOperator.execute()
