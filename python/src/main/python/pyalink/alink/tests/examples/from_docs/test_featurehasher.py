import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestFeatureHasher(unittest.TestCase):
    def test_featurehasher(self):

        df1 = pd.DataFrame([
            [1.1, True, "2", "A"],
            [1.1, False, "2", "B"],
            [1.1, True, "1", "B"],
            [2.2, True, "1", "A"]
        ])
        
        inOp = BatchOperator.fromDataframe(df1, schemaStr='double double, bool boolean, number int, str string')
        binarizer = FeatureHasher().setSelectedCols(["double", "bool", "number", "str"]).setOutputCol("output").setNumFeatures(200)
        binarizer.transform(inOp).print()
        
        df2 = pd.DataFrame([
            [1.1, True, "2", "A"],
            [1.1, False, "2", "B"],
            [1.1, True, "1", "B"],
            [2.2, True, "1", "A"]
        ])
        
        inOp1 = BatchOperator.fromDataframe(df2, schemaStr='double double, bool boolean, number int, str string')
        inOp2 = StreamOperator.fromDataframe(df2, schemaStr='double double, bool boolean, number int, str string')
        
        hasher = FeatureHasherBatchOp().setSelectedCols(["double", "bool", "number", "str"]).setOutputCol("output").setNumFeatures(200)
        hasher.linkFrom(inOp1).print()
        
        hasher = FeatureHasherStreamOp().setSelectedCols(["double", "bool", "number", "str"]).setOutputCol("output").setNumFeatures(200)
        hasher.linkFrom(inOp2).print()
        
        StreamOperator.execute()
        pass