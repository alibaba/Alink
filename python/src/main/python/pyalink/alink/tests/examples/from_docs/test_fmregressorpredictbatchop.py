import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestFmRegressorPredictBatchOp(unittest.TestCase):
    def test_fmregressorpredictbatchop(self):

        df = pd.DataFrame([
            ["1:1.1 3:2.0", 1.0],
            ["2:2.1 10:3.1", 1.0],
            ["1:1.2 5:3.2", 0.0],
            ["3:1.2 7:4.2", 0.0]
        ])
        
        input = BatchOperator.fromDataframe(df, schemaStr='kv string, label double')
        
        fm = FmRegressorTrainBatchOp()\
                .setVectorCol("kv")\
                .setLabelCol("label")
        model = input.link(fm)
        
        predictor = FmRegressorPredictBatchOp()\
                .setPredictionCol("pred")
        
        predictor.linkFrom(model, input).print()
        pass