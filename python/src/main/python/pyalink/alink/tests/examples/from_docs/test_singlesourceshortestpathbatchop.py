import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestSingleSourceShortestPathBatchOp(unittest.TestCase):
    def test_singlesourceshortestpathbatchop(self):

        df = pd.DataFrame([[1, 2],\
        [2, 3],\
        [3, 4],\
        [4, 5],\
        [5, 6],\
        [6, 7],\
        [7, 8],\
        [8, 9],\
        [9, 6]])
        
        data = BatchOperator.fromDataframe(df, schemaStr="source double, target double")
        SingleSourceShortestPathBatchOp()\
            .setEdgeSourceCol("source")\
            .setEdgeTargetCol("target")\
            .setSourcePoint("1")\
            .linkFrom(data)\
            .print()
        pass