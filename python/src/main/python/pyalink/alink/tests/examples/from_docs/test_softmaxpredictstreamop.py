import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestSoftmaxPredictStreamOp(unittest.TestCase):
    def test_softmaxpredictstreamop(self):

        df_data = pd.DataFrame([
               [2, 1, 1],
               [3, 2, 1],
               [4, 3, 2],
               [2, 4, 1],
               [2, 2, 1],
               [4, 3, 2],
               [1, 2, 1],
               [5, 3, 3]
        ])
        
        batchData = BatchOperator.fromDataframe(df_data, schemaStr='f0 int, f1 int, label int')
        dataTest = StreamOperator.fromDataframe(df_data, schemaStr='f0 int, f1 int, label int')
        colnames = ["f0","f1"]
        lr = SoftmaxTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
        model = batchData.link(lr)
        
        predictor = SoftmaxPredictStreamOp(model).setPredictionCol("pred")
        predictor.linkFrom(dataTest).print()
        StreamOperator.execute()
        pass