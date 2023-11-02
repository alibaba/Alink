import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestCommunityDetectionClusterBatchOp(unittest.TestCase):
    def test_communitydetectionclusterbatchop(self):

        df = pd.DataFrame([[3, 1],\
        [3, 0],\
        [0, 1],\
        [0, 2],\
        [2, 1],\
        [2, 4],\
        [5, 4],\
        [7, 4],\
        [5, 6],\
        [5, 8],\
        [5, 7],\
        [7, 8],\
        [6, 8],\
        [12, 10],\
        [12, 11],\
        [12, 13],\
        [12, 9],\
        [10, 9],\
        [8, 9],\
        [13, 9],\
        [10, 7],\
        [10, 11],\
        [11, 13]])
        
        edges = BatchOperator.fromDataframe(df, schemaStr="source int, target int")
        
        communityDetectionClusterBatchOp = CommunityDetectionClusterBatchOp()\
            .setEdgeSourceCol("source")\
            .setEdgeTargetCol("target")
        communityDetectionClusterBatchOp.linkFrom(edges).print()
        pass