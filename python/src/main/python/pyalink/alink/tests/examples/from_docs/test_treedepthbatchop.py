import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTreeDepthBatchOp(unittest.TestCase):
    def test_treedepthbatchop(self):

        df = pd.DataFrame([[0, 1],\
        [0, 2],\
        [1, 3],\
        [1, 4],\
        [2, 5],\
        [4, 6],\
        [7, 8],\
        [7, 9],\
        [9, 10],\
        [9, 11]])
        
        data = BatchOperator.fromDataframe(df, schemaStr="source double, target double")
        TreeDepthBatchOp()\
            .setEdgeSourceCol("source")\
            .setEdgeTargetCol("target")\
            .linkFrom(data)\
            .print()
        pass