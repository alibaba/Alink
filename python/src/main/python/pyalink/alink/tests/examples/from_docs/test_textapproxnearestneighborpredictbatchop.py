import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTextApproxNearestNeighborPredictBatchOp(unittest.TestCase):
    def test_textapproxnearestneighborpredictbatchop(self):

        df = pd.DataFrame([
            [0, "a b c d e", "a a b c e"],
            [1, "a a c e d w", "a a b b e d"],
            [2, "c d e f a", "b b c e f a"],
            [3, "b d e f h", "d d e a c"],
            [4, "a c e d m", "a e e f b c"]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='id long, text1 string, text2 string')
        
        train = TextApproxNearestNeighborTrainBatchOp().setIdCol("id").setSelectedCol("text1").setMetric("SIMHASH_HAMMING_SIM").linkFrom(inOp)
        predict = TextApproxNearestNeighborPredictBatchOp().setSelectedCol("text2").setTopN(3).linkFrom(train, inOp)
        predict.print()
        pass