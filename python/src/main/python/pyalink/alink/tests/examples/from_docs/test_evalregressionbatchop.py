import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestEvalRegressionBatchOp(unittest.TestCase):
    def test_evalregressionbatchop(self):

        df = pd.DataFrame([
            [0, 0],
            [8, 8],
            [1, 2],
            [9, 10],
            [3, 1],
            [10, 7]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='pred int, label int')
        
        metrics = EvalRegressionBatchOp().setPredictionCol("pred").setLabelCol("label").linkFrom(inOp).collectMetrics()
        
        print("Total Samples Number:", metrics.getCount())
        print("SSE:", metrics.getSse())
        print("SAE:", metrics.getSae())
        print("RMSE:", metrics.getRmse())
        print("R2:", metrics.getR2())
        pass