import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestConnectedComponentsBatchOp(unittest.TestCase):
    def test_connectedcomponentsbatchop(self):

        df = pd.DataFrame([[1, 2],\
        [2, 3],\
        [3, 4],\
        [4, 5],\
        [6, 7],\
        [7, 8],\
        [8, 9],\
        [9, 6]])
        
        data = BatchOperator.fromDataframe(df, schemaStr="source int, target int")
        ConnectedComponentsBatchOp()\
            .setEdgeSourceCol("source")\
            .setEdgeTargetCol("target")\
            .linkFrom(data)\
            .print()
        pass