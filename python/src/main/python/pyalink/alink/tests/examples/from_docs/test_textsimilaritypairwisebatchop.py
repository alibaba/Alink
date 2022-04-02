import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTextSimilarityPairwiseBatchOp(unittest.TestCase):
    def test_textsimilaritypairwisebatchop(self):

        df = pd.DataFrame([
            [0, "a b c d e", "a a b c e"],
            [1, "a a c e d w", "a a b b e d"],
            [2, "c d e f a", "b b c e f a"],
            [3, "b d e f h", "d d e a c"],
            [4, "a c e d m", "a e e f b c"]
        ])
        
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='id long, text1 string, text2 string')
        inOp2 = StreamOperator.fromDataframe(df, schemaStr='id long, text1 string, text2 string')
        
        batchOp = TextSimilarityPairwiseBatchOp().setSelectedCols(["text1", "text2"]).setMetric("LEVENSHTEIN").setOutputCol("output")
        batchOp.linkFrom(inOp1).print()
        
        streamOp = TextSimilarityPairwiseStreamOp().setSelectedCols(["text1", "text2"]).setMetric("COSINE").setOutputCol("output")
        streamOp.linkFrom(inOp2).print()
        StreamOperator.execute()
        pass