import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLassoRegPredictBatchOp(unittest.TestCase):
    def test_lassoregpredictbatchop(self):

        df = pd.DataFrame([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 3]
        ])
        
        batchData = BatchOperator.fromDataframe(df, schemaStr='f0 int, f1 int, label int')
        
        lasso = LassoRegTrainBatchOp()\
                .setLambda(0.1)\
                .setFeatureCols(["f0","f1"])\
                .setLabelCol("label")
        
        model = batchData.link(lasso)
        
        predictor = LassoRegPredictBatchOp()\
                .setPredictionCol("pred")
        
        predictor.linkFrom(model, batchData).print()
        pass