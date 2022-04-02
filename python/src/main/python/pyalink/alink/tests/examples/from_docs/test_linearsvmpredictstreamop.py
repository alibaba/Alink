import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLinearSvmPredictStreamOp(unittest.TestCase):
    def test_linearsvmpredictstreamop(self):

        df_data = pd.DataFrame([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 2]
        ])
        
        batchData = BatchOperator.fromDataframe(df_data, schemaStr='f0 int, f1 int, label int')
        streamData = StreamOperator.fromDataframe(df_data, schemaStr='f0 int, f1 int, label int')
        
        colnames = ["f0","f1"]
        lr = LinearSvmTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
        model = batchData.link(lr)
        
        predictor = LinearSvmPredictStreamOp(model).setPredictionCol("pred")
        predictor.linkFrom(streamData).print()
        StreamOperator.execute()
        pass