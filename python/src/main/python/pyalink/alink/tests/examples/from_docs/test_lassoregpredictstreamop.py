import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLassoRegPredictStreamOp(unittest.TestCase):
    def test_lassoregpredictstreamop(self):

        df = pd.DataFrame([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 3]])
        
        batchData = BatchOperator.fromDataframe(df, schemaStr='f0 double, f1 double, label double')
        streamData = StreamOperator.fromDataframe(df, schemaStr='f0 double, f1 double, label double')
        
        colnames = ["f0","f1"]
        lasso = LassoRegTrainBatchOp()\
                    .setLambda(0.1)\
                    .setFeatureCols(colnames)\
                    .setLabelCol("label")
        
        model = batchData.link(lasso)
        
        predictor = LassoRegPredictStreamOp(model)\
                    .setPredictionCol("pred")
        
        predictor.linkFrom(streamData).print()
        
        StreamOperator.execute()
        pass