import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorSliceBatchOp(unittest.TestCase):
    def test_vectorslicebatchop(self):

        df = pd.DataFrame([
            ["1:3,2:4,4:7", 1],
            ["0:3,5:5", 3],
            ["2:4,4:5", 4]
        ])
        
        data = BatchOperator.fromDataframe(df, schemaStr="vec string, id bigint")
        vecSlice = VectorSliceBatchOp().setSelectedCol("vec").setOutputCol("vec_slice").setIndices([1,2,3])
        vecSlice.linkFrom(data).collectToDataframe()
        pass