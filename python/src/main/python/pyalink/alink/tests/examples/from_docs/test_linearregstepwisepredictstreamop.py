import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLinearRegStepwisePredictStreamOp(unittest.TestCase):
    def test_linearregstepwisepredictstreamop(self):

        df = pd.DataFrame([
                    [16.3, 1.1, 1.1],
                    [16.8, 1.4, 1.5],
                    [19.2, 1.7, 1.8],
                    [18.0, 1.7, 1.7],
                    [19.5, 1.8, 1.9],
                    [20.9, 1.8, 1.8],
                    [21.1, 1.9, 1.8],
                    [20.9, 2.0, 2.1],
                    [20.3, 2.3, 2.4],
                    [22.0, 2.4, 2.5]])
        
        batchData = BatchOperator.fromDataframe(df, schemaStr='f0 double, f1 double, label double')
        streamData = StreamOperator.fromDataframe(df, schemaStr='f0 double, f1 double, label double')
        
        colnames = ["f0", "f1"]
        
        lrs = LinearRegStepwiseTrainBatchOp()\
                    .setFeatureCols(colnames)\
                    .setLabelCol("label")\
                    .setMethod("Forward")
        
        model = batchData.link(lrs)
        
        predictor = LinearRegStepwisePredictStreamOp(model)\
                    .setPredictionCol("pred")
        
        predictor.linkFrom(streamData).print()
        
        StreamOperator.execute()
        pass