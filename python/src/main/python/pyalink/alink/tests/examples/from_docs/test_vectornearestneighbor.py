import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorNearestNeighbor(unittest.TestCase):
    def test_vectornearestneighbor(self):

        df = pd.DataFrame([
            [0, "0 0 0"],
            [1, "1 1 1"],
            [2, "2 2 2"]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
        pipeline = VectorNearestNeighbor().setIdCol("id").setSelectedCol("vec").setTopN(3)
        
        pipeline.fit(inOp).transform(inOp).print()
        pass