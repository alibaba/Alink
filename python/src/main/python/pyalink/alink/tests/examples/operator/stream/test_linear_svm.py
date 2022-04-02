import unittest

from pyalink.alink import *


class TestYuhe(unittest.TestCase):

    def test_linear_svm_op_stream(self):
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

        streamData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='stream')
        batchData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')

        colnames = ["f0", "f1"]
        lr = LinearSvmTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
        model = batchData.link(lr)

        predictor = LinearSvmPredictStreamOp(model).setPredictionCol("pred")
        predictor.linkFrom(streamData).print()
