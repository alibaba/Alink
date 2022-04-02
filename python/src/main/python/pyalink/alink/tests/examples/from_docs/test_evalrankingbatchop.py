import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestEvalRankingBatchOp(unittest.TestCase):
    def test_evalrankingbatchop(self):

        df = pd.DataFrame([
            ["{\"object\":\"[1, 6, 2, 7, 8, 3, 9, 10, 4, 5]\"}", "{\"object\":\"[1, 2, 3, 4, 5]\"}"],
            ["{\"object\":\"[4, 1, 5, 6, 2, 7, 3, 8, 9, 10]\"}", "{\"object\":\"[1, 2, 3]\"}"],
            ["{\"object\":\"[1, 2, 3, 4, 5]\"}", "{\"object\":\"[]\"}"]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='pred string, label string')
        
        metrics = EvalRankingBatchOp().setPredictionCol('pred').setLabelCol('label').linkFrom(inOp).collectMetrics()
        print(metrics)
        pass