import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLogisticRegressionPredictStreamOp(unittest.TestCase):
    def test_logisticregressionpredictstreamop(self):

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
        
        dataTest = streamData
        colnames = ["f0","f1"]
        lr = LogisticRegressionTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
        model = batchData.link(lr)
        
        predictor = LogisticRegressionPredictStreamOp(model).setPredictionCol("pred")
        predictor.linkFrom(dataTest).print()
        StreamOperator.execute()
        pass