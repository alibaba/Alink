import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestYuhe(unittest.TestCase):

    def test_lasso_reg_op(self):
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

        batchData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')
        colnames = ["f0", "f1"]
        lasso = LassoRegTrainBatchOp().setLambda(0.1).setFeatureCols(colnames).setLabelCol("label")
        model = batchData.link(lasso)

        predictor = LassoRegPredictBatchOp().setPredictionCol("pred")
        predictor.linkFrom(model, batchData).print()
