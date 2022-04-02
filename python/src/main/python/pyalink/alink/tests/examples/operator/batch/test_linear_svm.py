import unittest

from pyalink.alink import *


class TestYuhe(unittest.TestCase):

    def test_linear_svm_op(self):
        import numpy as np
        import pandas as pd
        data = np.array([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 2]])
        df = pd.DataFrame({"f0": data[:, 0],
                           "f1": data[:, 1],
                           "label": data[:, 2]})

        input = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')
        dataTest = input
        colnames = ["f0", "f1"]
        svm = LinearSvmTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
        model = input.link(svm)

        from pyalink.alink.common.types.model_info import LinearClassifierModelInfo
        from pyalink.alink.common.types.train_info import LinearModelTrainInfo

        def model_info_callback(d: LinearClassifierModelInfo):
            self.assertEquals(type(d), LinearClassifierModelInfo)
            print(d.getFeatureNames())

        def train_info_callback(d: LinearModelTrainInfo):
            self.assertEquals(type(d), LinearModelTrainInfo)
            print(d.getWeight())

        svm.lazyCollectModelInfo(model_info_callback)
        svm.lazyCollectTrainInfo(train_info_callback)

        predictor = LinearSvmPredictBatchOp().setPredictionCol("pred")
        predictor.linkFrom(model, dataTest).print()
