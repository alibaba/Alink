import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestStringNearestNeighborPredictBatchOp(unittest.TestCase):
    def test_stringnearestneighborpredictbatchop(self):

        df = pd.DataFrame([
            [0, "abcde", "aabce"],
            [1, "aacedw", "aabbed"],
            [2, "cdefa", "bbcefa"],
            [3, "bdefh", "ddeac"],
            [4, "acedm", "aeefbc"]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='id long, text1 string, text2 string')
        
        train = StringNearestNeighborTrainBatchOp().setIdCol("id").setSelectedCol("text1").setMetric("LEVENSHTEIN_SIM").linkFrom(inOp)
        predict = StringNearestNeighborPredictBatchOp().setSelectedCol("text2").setTopN(3).linkFrom(train, inOp)
        predict.print()
        pass