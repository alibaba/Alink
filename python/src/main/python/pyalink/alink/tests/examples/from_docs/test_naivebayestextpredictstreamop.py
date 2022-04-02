import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestNaiveBayesTextPredictStreamOp(unittest.TestCase):
    def test_naivebayestextpredictstreamop(self):

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
        
        # stream data
        streamData = StreamOperator.fromDataframe(df_data, schemaStr='sv string, dv string, label string')
        # train op
        ns = NaiveBayesTextTrainBatchOp().setVectorCol("sv").setLabelCol("label")
        model = batchData.link(ns)
        # predict op
        predictor = NaiveBayesTextPredictStreamOp(model).setVectorCol("sv").setReservedCols(["sv", "label"]).setPredictionCol("pred")
        predictor.linkFrom(streamData).print()
        StreamOperator.execute()
        pass