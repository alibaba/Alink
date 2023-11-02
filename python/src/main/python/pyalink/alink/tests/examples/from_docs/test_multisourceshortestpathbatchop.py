import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestMultiSourceShortestPathBatchOp(unittest.TestCase):
    def test_multisourceshortestpathbatchop(self):

        df = pd.DataFrame([[0, 1, 0.4],
                           [1, 2, 1.3],
                           [2, 3, 1.0],
                           [3, 4, 1.0],
                           [4, 5, 1.0],
                           [5, 6, 1.0],
                           [6, 7, 1.0],
                           [7, 8, 1.0],
                           [8, 9, 1.0],
                           [9, 6, 1.0],
                           [19, 16, 1.0]])
        source_df = pd.DataFrame([[1, 1],
                                  [5, 5]])
        data = BatchOperator.fromDataframe(df, schemaStr="source int, target int, weight double")
        source_data = BatchOperator.fromDataframe(source_df, schemaStr="source int, target int")
        
        MultiSourceShortestPathBatchOp()\
            .setEdgeSourceCol("source")\
            .setEdgeTargetCol("target")\
            .setEdgeWeightCol("weight")\
            .setSourcePointCol("source")\
            .linkFrom(data, source_data)\
            .print()
        pass