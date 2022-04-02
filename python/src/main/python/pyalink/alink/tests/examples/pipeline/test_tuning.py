import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestTuning(unittest.TestCase):

    def init_data(self):
        data = np.array([
            [1.0, "A", 0, 0, 0],
            [2.0, "B", 1, 1, 0],
            [3.0, "C", 2, 2, 1],
            [4.0, "D", 3, 3, 1],
            [1.0, "A", 0, 0, 0],
            [2.0, "B", 1, 1, 0],
            [3.0, "C", 2, 2, 1],
            [4.0, "D", 3, 3, 1],
            [1.0, "A", 0, 0, 0],
            [2.0, "B", 1, 1, 0],
            [3.0, "C", 2, 2, 1]
        ])
        df = pd.DataFrame({
            "f0": data[:, 0],
            "f1": data[:, 1],
            "f2": data[:, 2],
            "f3": data[:, 3],
            "label": data[:, 4],
        })
        self.data = dataframeToOperator(df, schemaStr="f0 float, f1 string, f2 long, f3 long, label int",
                                        op_type="batch")

    def setUp(self):
        self.init_data()

    def test_GridSearchCV(self):
        colNames = ["f0", "f1", "f2", "f3", "label"]
        featureColNames = [colNames[0], colNames[1], colNames[2], colNames[3]]
        categoricalColNames = [colNames[1]]
        labelColName = colNames[4]

        rf = RandomForestClassifier() \
            .setFeatureCols(featureColNames) \
            .setCategoricalCols(categoricalColNames) \
            .setLabelCol(labelColName) \
            .setPredictionCol("pred_result") \
            .setPredictionDetailCol("pred_detail") \
            .setSubsamplingRatio(1.0)

        pipeline = Pipeline(rf)

        paramGrid = ParamGrid() \
            .addGrid(rf, 'SUBSAMPLING_RATIO', [1.0, 1.0, 1.0]) \
            .addGrid(rf, 'NUM_TREES', [3, 6, 9])

        tuning_evaluator = BinaryClassificationTuningEvaluator() \
            .setLabelCol(labelColName) \
            .setPredictionDetailCol("pred_detail") \
            .setTuningBinaryClassMetric("Accuracy")

        cv = GridSearchCV() \
            .setEstimator(pipeline) \
            .setParamGrid(paramGrid) \
            .setTuningEvaluator(tuning_evaluator) \
            .setNumFolds(2)

        cvModel = cv.fit(self.data)
        self.assertTrue(isinstance(cvModel, GridSearchCVModel))
        cvModel.transform(self.data).print()

    def test_GridSearchTVSplit(self):
        colNames = ["f0", "f1", "f2", "f3", "label"]
        featureColNames = [colNames[0], colNames[1], colNames[2], colNames[3]]
        categoricalColNames = [colNames[1]]
        labelColName = colNames[4]

        rf = RandomForestClassifier() \
            .setFeatureCols(featureColNames) \
            .setCategoricalCols(categoricalColNames) \
            .setLabelCol(labelColName) \
            .setPredictionCol("pred_result") \
            .setPredictionDetailCol("pred_detail") \
            .setSubsamplingRatio(1.0)

        pipeline = Pipeline(rf)

        paramGrid = ParamGrid() \
            .addGrid(rf, 'SUBSAMPLING_RATIO', [1.0, 1.0, 1.0]) \
            .addGrid(rf, 'NUM_TREES', [3, 6, 9])

        tuning_evaluator = BinaryClassificationTuningEvaluator() \
            .setLabelCol(labelColName) \
            .setPredictionDetailCol("pred_detail") \
            .setTuningBinaryClassMetric("Accuracy")

        cv = GridSearchTVSplit() \
            .setEstimator(pipeline) \
            .setParamGrid(paramGrid) \
            .setTuningEvaluator(tuning_evaluator) \
            .setTrainRatio(0.8)

        cvModel = cv.fit(self.data)
        self.assertTrue(isinstance(cvModel, GridSearchTVSplitModel))
        cvModel.transform(self.data).print()

    def test_RandomSearchCV(self):
        colNames = ["f0", "f1", "f2", "f3", "label"]
        featureColNames = [colNames[0], colNames[1], colNames[2], colNames[3]]
        categoricalColNames = [colNames[1]]
        labelColName = colNames[4]

        rf = RandomForestClassifier() \
            .setFeatureCols(featureColNames) \
            .setCategoricalCols(categoricalColNames) \
            .setLabelCol(labelColName) \
            .setPredictionCol("pred_result") \
            .setPredictionDetailCol("pred_detail") \
            .setSubsamplingRatio(1.0)

        pipeline = Pipeline(rf)

        param_dist = ParamDist()\
            .addDist(rf, 'SUBSAMPLING_RATIO', ValueDist.randArray([1.0, 0.9, 0.8]))\
            .addDist(rf, 'NUM_TREES', ValueDist.randInteger(1, 4))

        tuning_evaluator = BinaryClassificationTuningEvaluator() \
            .setLabelCol(labelColName) \
            .setPredictionDetailCol("pred_detail") \
            .setTuningBinaryClassMetric("Accuracy")

        cv = RandomSearchCV() \
            .setEstimator(pipeline) \
            .setParamDist(param_dist) \
            .setTuningEvaluator(tuning_evaluator) \
            .setNumFolds(2)

        cvModel = cv.fit(self.data)
        self.assertTrue(isinstance(cvModel, RandomSearchCVModel))
        cvModel.transform(self.data).print()

    def test_RandomSearchCVSplit(self):
        colNames = ["f0", "f1", "f2", "f3", "label"]
        featureColNames = [colNames[0], colNames[1], colNames[2], colNames[3]]
        categoricalColNames = [colNames[1]]
        labelColName = colNames[4]

        rf = RandomForestClassifier() \
            .setFeatureCols(featureColNames) \
            .setCategoricalCols(categoricalColNames) \
            .setLabelCol(labelColName) \
            .setPredictionCol("pred_result") \
            .setPredictionDetailCol("pred_detail") \
            .setSubsamplingRatio(1.0)

        pipeline = Pipeline(rf)

        param_dist = ParamDist()\
            .addDist(rf, 'SUBSAMPLING_RATIO', ValueDist.randArray([1.0, 0.9, 0.8]))\
            .addDist(rf, 'NUM_TREES', ValueDist.randInteger(1, 4))

        tuning_evaluator = BinaryClassificationTuningEvaluator() \
            .setLabelCol(labelColName) \
            .setPredictionDetailCol("pred_detail") \
            .setTuningBinaryClassMetric("Accuracy")

        cv = RandomSearchTVSplit() \
            .setEstimator(pipeline) \
            .setParamDist(param_dist) \
            .setTuningEvaluator(tuning_evaluator) \
            .setTrainRatio(0.8)

        cvModel = cv.fit(self.data)
        self.assertTrue(isinstance(cvModel, RandomSearchTVSplitModel))
        cvModel.transform(self.data).print()
