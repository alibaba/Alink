import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestStringNearestNeighbor(unittest.TestCase):
    def test_stringnearestneighbor(self):

        df = pd.DataFrame([
            [0, "abcde", "aabce"],
            [1, "aacedw", "aabbed"],
            [2, "cdefa", "bbcefa"],
            [3, "bdefh", "ddeac"],
            [4, "acedm", "aeefbc"]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='id long, text1 string, text2 string')
        
        pipeline = StringNearestNeighbor().setIdCol("id").setSelectedCol("text1").setMetric("LEVENSHTEIN_SIM").setTopN(3)
        
        pipeline.fit(inOp).transform(inOp).print()
        pass