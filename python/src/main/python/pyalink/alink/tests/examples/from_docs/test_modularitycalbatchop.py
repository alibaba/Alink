import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestModularityCalBatchOp(unittest.TestCase):
    def test_modularitycalbatchop(self):

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
        
        df2 = pd.DataFrame([[2, 0],\
                [4, 1],\
                [7, 1],\
                [8, 1],\
                [9, 2],\
                [10, 2]])
        
        vertices = BatchOperator.fromDataframe(df2, schemaStr="vertex int, label bigint")
        
        communityDetectionClassify = CommunityDetectionClassifyBatchOp()\
            .setEdgeSourceCol("source")\
            .setEdgeTargetCol("target")\
            .setVertexCol("vertex")\
            .setVertexLabelCol("label")
        community = communityDetectionClassify.linkFrom(edges, vertices)
        
        
        modularityCal = ModularityCalBatchOp()\
            .setEdgeSourceCol("source")\
            .setEdgeTargetCol("target")\
            .setVertexCol("vertex")\
            .setVertexCommunityCol("label")
        modularityCal.linkFrom(edges, community).print()
        pass