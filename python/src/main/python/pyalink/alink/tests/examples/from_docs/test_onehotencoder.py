import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestOneHotEncoder(unittest.TestCase):
    def test_onehotencoder(self):

        df = pd.DataFrame([
            ["a", 1],
            ["b", 1],
            ["c", 1],
            ["e", 2],
            ["a", 2],
            ["b", 1],
            ["c", 2],
            ["d", 2],
            [None, 1]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='query string, weight long')
        
        # one hot train
        one_hot = OneHotEncoder().setSelectedCols(["query"]).setOutputCols(["output"])
        one_hot.fit(inOp).transform(inOp).print()
        pass