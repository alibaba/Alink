import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLinearRegPredictStreamOp(unittest.TestCase):
    def test_linearregpredictstreamop(self):

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
        streamData = StreamOperator.fromDataframe(df, schemaStr='f0 int, f1 int, label int')
        colnames = ["f0","f1"]
        lr = LinearRegTrainBatchOp()\
                    .setFeatureCols(colnames)\
                    .setLabelCol("label")
        
        model = batchData.link(lr)
        
        predictor = LinearRegPredictStreamOp(model)\
                     .setPredictionCol("pred")
        
        predictor.linkFrom(streamData).print()
        
        StreamOperator.execute()
        pass