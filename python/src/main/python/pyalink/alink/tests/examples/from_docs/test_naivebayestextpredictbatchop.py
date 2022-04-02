import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestNaiveBayesTextPredictBatchOp(unittest.TestCase):
    def test_naivebayestextpredictbatchop(self):

        df_data = pd.DataFrame([
                  ["$31$0:1.0 1:1.0 2:1.0 30:1.0","1.0  1.0  1.0  1.0", '1'],
                  ["$31$0:1.0 1:1.0 2:0.0 30:1.0","1.0  1.0  0.0  1.0", '1'],
                  ["$31$0:1.0 1:0.0 2:1.0 30:1.0","1.0  0.0  1.0  1.0", '1'],
                  ["$31$0:1.0 1:0.0 2:1.0 30:1.0","1.0  0.0  1.0  1.0", '1'],
                  ["$31$0:0.0 1:1.0 2:1.0 30:0.0","0.0  1.0  1.0  0.0", '0'],
                  ["$31$0:0.0 1:1.0 2:1.0 30:0.0","0.0  1.0  1.0  0.0", '0'],
                  ["$31$0:0.0 1:1.0 2:1.0 30:0.0","0.0  1.0  1.0  0.0", '0']
        ])
        
        batchData = BatchOperator.fromDataframe(df_data, schemaStr='sv string, dv string, label string')
        
        # train op
        ns = NaiveBayesTextTrainBatchOp().setVectorCol("sv").setLabelCol("label")
        model = batchData.link(ns)
        # predict op
        predictor = NaiveBayesTextPredictBatchOp().setVectorCol("sv").setReservedCols(["sv", "label"]).setPredictionCol("pred")
        predictor.linkFrom(model, batchData).print()
        
        pass