import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestRidgeRegPredictBatchOp(unittest.TestCase):
    def test_ridgeregpredictbatchop(self):

        df = pd.DataFrame([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 3]])
        
        batchData = BatchOperator.fromDataframe(df, schemaStr='f0 int, f1 int, label int')
        ridge = RidgeRegTrainBatchOp()\
            .setLambda(0.1)\
            .setFeatureCols(["f0","f1"])\
            .setLabelCol("label")
        model = batchData.link(ridge)
        
        predictor = RidgeRegPredictBatchOp()\
            .setPredictionCol("pred")
        predictor.linkFrom(model, batchData).print()
        pass