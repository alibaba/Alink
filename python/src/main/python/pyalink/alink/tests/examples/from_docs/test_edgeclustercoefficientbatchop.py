import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestEdgeClusterCoefficientBatchOp(unittest.TestCase):
    def test_edgeclustercoefficientbatchop(self):

        df = pd.DataFrame([[1.0, 2.0],\
        [1.0, 3.0],\
        [3.0, 2.0],\
        [5.0, 2.0],\
        [3.0, 4.0],\
        [4.0, 2.0],\
        [5.0, 4.0],\
        [5.0, 1.0],\
        [5.0, 3.0],\
        [5.0, 6.0],\
        [5.0, 8.0],\
        [7.0, 6.0],\
        [7.0, 1.0],\
        [7.0, 5.0],\
        [8.0, 6.0],\
        [8.0, 4.0]])
        
        data = BatchOperator.fromDataframe(df, schemaStr="source double, target double")
        EdgeClusterCoefficientBatchOp()\
            .setEdgeSourceCol("source")\
            .setEdgeTargetCol("target")\
            .linkFrom(data)\
            .print()
        pass