import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTriangleListBatchOp(unittest.TestCase):
    def test_trianglelistbatchop(self):

        df = pd.DataFrame([[1.0,2.0],\
        [1.0,3.0],\
        [1.0,4.0],\
        [1.0,5.0],\
        [1.0,6.0],\
        [2.0,3.0],\
        [4.0,3.0],\
        [5.0,4.0],\
        [5.0,6.0],\
        [5.0,7.0],\
        [6.0,7.0]])
        
        data = BatchOperator.fromDataframe(df, schemaStr="source double, target double")
        TriangleListBatchOp()\
            .setEdgeSourceCol("source")\
            .setEdgeTargetCol("target")\
            .linkFrom(data)\
            .print()
        pass