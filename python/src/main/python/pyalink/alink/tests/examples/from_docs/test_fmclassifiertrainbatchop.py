import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestFmClassifierTrainBatchOp(unittest.TestCase):
    def test_fmclassifiertrainbatchop(self):

        df = pd.DataFrame([
               ["1:1.1 3:2.0", 1.0],
               ["2:2.1 10:3.1", 1.0],
               ["1:1.2 5:3.2", 0.0],
               ["3:1.2 7:4.2", 0.0]
        ])
        
        input = BatchOperator.fromDataframe(df, schemaStr='kv string, label double')
        dataTest = input
        
        # load data
        dataTest = input
        fm = FmClassifierTrainBatchOp().setVectorCol("kv").setLabelCol("label")
        model = input.link(fm)
        
        predictor = FmClassifierPredictBatchOp().setPredictionCol("pred")
        predictor.linkFrom(model, dataTest).print()
        pass