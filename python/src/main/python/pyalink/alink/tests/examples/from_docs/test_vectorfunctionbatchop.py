import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorFunctionBatchOp(unittest.TestCase):
    def test_vectorfunctionbatchop(self):

        df = pd.DataFrame([
            [1,"16.3, 1.1, 1.1"],
            [2,"16.8, 1.4, 1.5"],
            [3,"19.2, 1.7, 1.8"],
            [4,"10.0, 1.7, 1.7"],
            [5,"19.5, 1.8, 1.9"],
            [6,"20.9, 1.8, 1.8"],
            [7,"21.1, 1.9, 1.8"],
            [8,"20.9, 2.0, 2.1"],
            [9,"20.3, 2.3, 2.4"],
            [10,"22.0, 2.4, 2.5"]
        ])
        
        opData = BatchOperator.fromDataframe(df, schemaStr="id bigint, vec string")
        result = VectorFunctionBatchOp().setSelectedCol("vec")\
        .setOutputCol("out").setFuncName("max").linkFrom(opData)
        result.collectToDataframe()
        pass