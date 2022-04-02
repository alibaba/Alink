import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorAssemblerBatchOp(unittest.TestCase):
    def test_vectorassemblerbatchop(self):

        df = pd.DataFrame([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 3]
        ])
        
        data = BatchOperator.fromDataframe(df, schemaStr="f0 int, f1 int, f2 int")
        colnames = ["f0","f1","f2"]
        VectorAssemblerBatchOp().setSelectedCols(colnames)\
        .setOutputCol("out").linkFrom(data).print()
        pass