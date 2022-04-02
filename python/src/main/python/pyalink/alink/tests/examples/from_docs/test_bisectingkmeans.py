import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestBisectingKMeans(unittest.TestCase):
    def test_bisectingkmeans(self):

        df = pd.DataFrame([
            [0, "0 0 0"],
            [1, "0.1,0.1,0.1"],
            [2, "0.2,0.2,0.2"],
            [3, "9 9 9"],
            [4, "9.1 9.1 9.1"],
            [5, "9.2 9.2 9.2"]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
        
        kmeans = BisectingKMeans()\
            .setVectorCol("vec")\
            .setK(2)\
            .setPredictionCol("pred")
        
        kmeans.fit(inOp)\
            .transform(inOp)\
            .print()
        pass