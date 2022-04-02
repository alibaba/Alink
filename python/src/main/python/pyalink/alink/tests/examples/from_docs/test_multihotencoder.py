import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestMultiHotEncoder(unittest.TestCase):
    def test_multihotencoder(self):

        df = pd.DataFrame([
            ["a b", 1],
            ["b c", 1],
            ["c d", 1],
            ["a d", 2],
            ["d e", 2],
            [None, 1]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='query string, weight long')
        
        multi_hot = MultiHotEncoder().setSelectedCols(["query"]).setOutputCols(["output"])
        multi_hot.fit(inOp).transform(inOp).print()
        pass