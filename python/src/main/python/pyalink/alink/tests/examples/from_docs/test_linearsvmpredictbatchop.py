import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLinearSvmPredictBatchOp(unittest.TestCase):
    def test_linearsvmpredictbatchop(self):

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
        
        dataTest = input
        colnames = ["f0","f1"]
        svm = LinearSvmTrainBatchOp().setFeatureCols(colnames).setLabelCol("label")
        model = input.link(svm)
        
        predictor = LinearSvmPredictBatchOp().setPredictionCol("pred")
        predictor.linkFrom(model, dataTest).print()
        pass