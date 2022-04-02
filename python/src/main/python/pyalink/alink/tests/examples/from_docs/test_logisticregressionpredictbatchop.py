import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLogisticRegressionPredictBatchOp(unittest.TestCase):
    def test_logisticregressionpredictbatchop(self):

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
        
        input = BatchOperator.fromDataframe(df_data, schemaStr='f0 int, f1 int, label int')
        
        # load data
        dataTest = input
        colnames = ["f0","f1"]
        lr = LogisticRegressionTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
        model = input.link(lr)
        
        predictor = LogisticRegressionPredictBatchOp().setPredictionCol("pred")
        predictor.linkFrom(model, dataTest).print()
        pass