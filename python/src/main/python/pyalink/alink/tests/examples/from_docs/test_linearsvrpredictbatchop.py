import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLinearSvrPredictBatchOp(unittest.TestCase):
    def test_linearsvrpredictbatchop(self):

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
            [22.0, 2.4, 2.5]
        ])
        
        batchSource = BatchOperator.fromDataframe(df, schemaStr=' y double, x1 double, x2 double')
        
        lsvr = LinearSvrTrainBatchOp()\
            .setFeatureCols(["x1", "x2"])\
            .setLabelCol("y")\
            .setC(1.0)\
            .setTau(0.01)
        
        model = batchSource.link(lsvr)
        
        predictor = LinearSvrPredictBatchOp()\
            .setPredictionCol("pred")
        
        predictor.linkFrom(model, batchSource).print()
        pass