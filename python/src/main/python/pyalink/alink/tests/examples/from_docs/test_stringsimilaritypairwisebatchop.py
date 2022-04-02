import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestStringSimilarityPairwiseBatchOp(unittest.TestCase):
    def test_stringsimilaritypairwisebatchop(self):

        df = pd.DataFrame([
            [0, "abcde", "aabce"],
            [1, "aacedw", "aabbed"],
            [2, "cdefa", "bbcefa"],
            [3, "bdefh", "ddeac"],
            [4, "acedm", "aeefbc"]
        ])
        
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='id long, text1 string, text2 string')
        inOp2 = StreamOperator.fromDataframe(df, schemaStr='id long, text1 string, text2 string')
        
        batchOp = StringSimilarityPairwiseBatchOp().setSelectedCols(["text1", "text2"]).setMetric("LEVENSHTEIN").setOutputCol("LEVENSHTEIN")
        batchOp.linkFrom(inOp1).print()
        
        streamOp = StringSimilarityPairwiseStreamOp().setSelectedCols(["text1", "text2"]).setMetric("COSINE").setOutputCol("COSINE")
        streamOp.linkFrom(inOp2).print()
        StreamOperator.execute()
        pass