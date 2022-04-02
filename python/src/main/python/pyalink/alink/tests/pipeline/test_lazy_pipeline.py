import io
import sys
import tempfile
import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestTrainerLazyTrainInfo(unittest.TestCase):

    def setUp(self) -> None:
        self.saved_stdout = sys.stdout
        self.str_out = io.StringIO()
        sys.stdout = self.str_out

    def tearDown(self) -> None:
        sys.stdout = self.saved_stdout
        print(self.str_out.getvalue())

    def test_trainer_lazy_train_info(self):
        data = np.array([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 3]])
        df = pd.DataFrame({"f0": data[:, 0],
                           "f1": data[:, 1],
                           "label": data[:, 2]})
        colnames = ["f0", "f1"]
        source = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')

        title = "===== TITLE in TRAINER LAZY_PRINT_TRAIN_INFO ====="

        trainer = LassoRegression().setLambda(0.1).setFeatureCols(colnames).setLabelCol("label").setPredictionCol("pred")
        trainer.enableLazyPrintTrainInfo(title)
        model: LassoRegressionModel = trainer.fit(source)
        output = model.transform(source)
        output.print()

        content = self.str_out.getvalue()
        self.assertTrue(title in content)

    def test_trainer_lazy_model_info(self):
        data = np.array([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 3]])
        df = pd.DataFrame({"f0": data[:, 0],
                           "f1": data[:, 1],
                           "label": data[:, 2]})
        colnames = ["f0", "f1"]
        source = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')

        title = "===== TITLE in TRAINER LAZY_PRINT_MODEL_INFO ====="

        trainer = LassoRegression().setLambda(0.1).setFeatureCols(colnames).setLabelCol("label").setPredictionCol("pred")
        trainer.enableLazyPrintModelInfo(title)
        model: LassoRegressionModel = trainer.fit(source)
        output = model.transform(source)
        output.print()

        content = self.str_out.getvalue()
        self.assertTrue(title in content)

    def test_trainer_lazy_transform_data(self):
        data = np.array([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 3]])
        df = pd.DataFrame({"f0": data[:, 0],
                           "f1": data[:, 1],
                           "label": data[:, 2]})
        colnames = ["f0", "f1"]
        source = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')

        title = "===== TITLE in TRAINER LAZY_PRINT_TRANSFORM_DATA ====="

        trainer = LassoRegression().setLambda(0.1).setFeatureCols(colnames).setLabelCol("label").setPredictionCol("pred")
        trainer.enableLazyPrintTransformData(3, title)
        model: LassoRegressionModel = trainer.fit(source)
        output = model.transform(source)
        output.print()

        content = self.str_out.getvalue()
        self.assertTrue(title in content)

    def test_trainer_lazy_transform_stat(self):
        data = np.array([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 3]])
        df = pd.DataFrame({"f0": data[:, 0],
                           "f1": data[:, 1],
                           "label": data[:, 2]})
        colnames = ["f0", "f1"]
        source = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')

        title = "===== TITLE in TRAINER LAZY_PRINT_TRANSFORM_STAT ====="

        trainer = LassoRegression().setLambda(0.1).setFeatureCols(colnames).setLabelCol("label").setPredictionCol("pred")
        trainer.enableLazyPrintTransformStat(title)
        model: LassoRegressionModel = trainer.fit(source)
        output = model.transform(source)
        output.print()

        content = self.str_out.getvalue()
        self.assertTrue(title in content)

    def test_transformer_lazy_transform_data(self):
        data = np.array([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 3]])
        df = pd.DataFrame({"f0": data[:, 0],
                           "f1": data[:, 1],
                           "label": data[:, 2]})
        colnames = ["f0", "f1"]
        source = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')

        title = "===== TITLE in TRANSFORMER LAZY_PRINT_TRANSFORM_DATA ====="

        trainer = LassoRegression().setLambda(0.1).setFeatureCols(colnames).setLabelCol("label").setPredictionCol("pred")
        model: LassoRegressionModel = trainer.fit(source)
        model.enableLazyPrintTransformData(3, title)
        output = model.transform(source)
        output.print()

        content = self.str_out.getvalue()
        self.assertTrue(title in content)

    def test_transformer_lazy_transform_stat(self):
        data = np.array([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 3]])
        df = pd.DataFrame({"f0": data[:, 0],
                           "f1": data[:, 1],
                           "label": data[:, 2]})
        colnames = ["f0", "f1"]
        source = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')

        title = "===== TITLE in TRANSFORMER LAZY_PRINT_TRANSFORM_STAT ====="

        trainer = LassoRegression().setLambda(0.1).setFeatureCols(colnames).setLabelCol("label").setPredictionCol("pred")
        model: LassoRegressionModel = trainer.fit(source)
        model.enableLazyPrintTransformStat(title)
        output = model.transform(source)
        output.print()

        content = self.str_out.getvalue()
        self.assertTrue(title in content)

    def test_transformer_lazy_transform_stat_in_pipeline_model(self):
        do_save = True
        do_load = True

        lfs = LocalFileSystem()
        filepath = FilePath(Path("tmp/test_alink_file_sink_filepath"), lfs)
        model_sink = AkSinkBatchOp() \
            .setFilePath(filepath) \
            .setOverwriteSink(True) \
            .setNumFiles(3)
        model_source = AkSourceBatchOp() \
            .setFilePath(filepath)

        data = np.array([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 3]])
        df = pd.DataFrame({"f0": data[:, 0],
                           "f1": data[:, 1],
                           "label": data[:, 2]})
        colnames = ["f0", "f1"]
        source = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')

        title1 = "===== TITLE in TRANSFORMER LAZY_PRINT_TRANSFORM_STAT ====="
        title2 = "===== TITLE in ESTIMATOR LAZY_PRINT_TRANSFORM_STAT ====="
        title3 = "===== TITLE in ESTIMATOR LAZY_PRINT_TRAIN_INFO ====="

        if do_save:
            select = Select().setClause("*, f0 + 100 as f2")
            select.enableLazyPrintTransformStat(title1)
            trainer = LassoRegression().setLambda(0.1).setFeatureCols(colnames).setLabelCol("label").setPredictionCol("pred")
            trainer.enableLazyPrintTransformData(3, title2)
            trainer.enableLazyPrintTrainInfo(title3)

            pipeline = Pipeline()
            pipeline.add(select)
            pipeline.add(trainer)
            pipeline_model: PipelineModel = pipeline.fit(source)
            pipeline_model.save().link(model_sink)
            BatchOperator.execute()

            content = self.str_out.getvalue()
            self.assertTrue(title3 in content)
            self.assertFalse(title1 in content)
            self.assertFalse(title2 in content)

            if do_load:
                self.str_out.truncate(0)
                self.str_out.seek(0)

        if do_load:
            pipeline_model = PipelineModel.collectLoad(model_source)
            output = pipeline_model.transform(source)
            output.print()

            content = self.str_out.getvalue()
            self.assertTrue(title1 in content)
            self.assertTrue(title2 in content)

    def test_trainer_lazy_multiple_fit(self):
        data = np.array([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 3]])
        df = pd.DataFrame({"f0": data[:, 0],
                           "f1": data[:, 1],
                           "label": data[:, 2]})
        colnames = ["f0", "f1"]
        source = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')

        title = "===== TITLE in TRAINER LAZY_PRINT_TRAIN_INFO ====="

        trainer = LassoRegression().setLambda(0.1).setFeatureCols(colnames).setLabelCol("label").setPredictionCol("pred")
        trainer.enableLazyPrintTrainInfo(title)
        model: LassoRegressionModel = trainer.fit(source)
        model2: LassoRegressionModel = trainer.fit(source)
        output = model.transform(source)
        output.print()

        content = self.str_out.getvalue()
        self.assertTrue(content.count(title) == 2)

    def test_trainer_multiple_fit_and_transform(self):
        data = np.array([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 3]])
        df = pd.DataFrame({"f0": data[:, 0],
                           "f1": data[:, 1],
                           "label": data[:, 2]})
        colnames = ["f0", "f1"]
        source = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')

        title = "===== TITLE in TRANSFORMER LAZY_PRINT_TRANSFORM_DATA ====="

        trainer = LassoRegression().setLambda(0.1).setFeatureCols(colnames).setLabelCol("label").setPredictionCol("pred")
        trainer.enableLazyPrintTransformData(3, title)
        model: LassoRegressionModel = trainer.fit(source)
        model2: LassoRegressionModel = trainer.fit(source)
        output = model.transform(source)
        model.transform(source)
        model2.transform(source)
        model2.transform(source)
        output.print()

        content = self.str_out.getvalue()
        self.assertTrue(content.count(title) == 4)

    def test_lazy_in_pipeline_load(self):
        do_save = True
        do_load = True

        pipeline_save_filename = tempfile.NamedTemporaryFile().name

        lfs = LocalFileSystem()
        filepath = FilePath(Path(pipeline_save_filename), lfs)
        model_sink = AkSinkBatchOp() \
            .setFilePath(filepath) \
            .setOverwriteSink(True) \
            .setNumFiles(3)
        model_source = AkSourceBatchOp() \
            .setFilePath(filepath)

        data = np.array([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 3]])
        df = pd.DataFrame({"f0": data[:, 0],
                           "f1": data[:, 1],
                           "label": data[:, 2]})
        colnames = ["f0", "f1"]
        source = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')

        title1 = "===== TITLE in TRANSFORMER LAZY_PRINT_TRANSFORM_STAT ====="
        title2 = "===== TITLE in ESTIMATOR LAZY_PRINT_TRANSFORM_STAT ====="
        title3 = "===== TITLE in ESTIMATOR LAZY_PRINT_TRAIN_INFO ====="

        if do_save:
            select = Select().setClause("*, f0 + 100 as f2")
            select.enableLazyPrintTransformStat(title1)
            trainer = LassoRegression().setLambda(0.1).setFeatureCols(colnames).setLabelCol("label").setPredictionCol("pred")
            trainer.enableLazyPrintTransformData(3, title2)
            trainer.enableLazyPrintTrainInfo(title3)

            pipeline = Pipeline()
            pipeline.add(select)
            pipeline.add(trainer)

            pipeline.save(pipeline_save_filename)
            BatchOperator.execute()

            content = self.str_out.getvalue()
            self.assertFalse(title3 in content)
            self.assertFalse(title1 in content)
            self.assertFalse(title2 in content)

            if do_load:
                self.str_out.truncate(0)
                self.str_out.seek(0)

        if do_load:
            pipeline = Pipeline.collectLoad(model_source)
            pipeline_model = pipeline.fit(source)
            output = pipeline_model.transform(source)
            output.print()

            content = self.str_out.getvalue()
            self.assertTrue(title1 in content)
            self.assertTrue(title2 in content)
