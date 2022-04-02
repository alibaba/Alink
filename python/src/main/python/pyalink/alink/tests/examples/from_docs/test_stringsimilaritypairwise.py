import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestStringSimilarityPairwise(unittest.TestCase):
    def test_stringsimilaritypairwise(self):

        df = pd.DataFrame([
            [0, "abcde", "aabce"],
            [1, "aacedw", "aabbed"],
            [2, "cdefa", "bbcefa"],
            [3, "bdefh", "ddeac"],
            [4, "acedm", "aeefbc"]
        ])
        
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='id long, text1 string, text2 string')
        inOp2 = StreamOperator.fromDataframe(df, schemaStr='id long, text1 string, text2 string')
        
        stringSimilarityPairwise = StringSimilarityPairwise().setSelectedCols(["text1", "text2"]).setMetric("LEVENSHTEIN").setOutputCol("output")
        stringSimilarityPairwise.transform(inOp1).print()
        stringSimilarityPairwise.transform(inOp2).print()
        StreamOperator.execute()
        pass