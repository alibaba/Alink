import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVertexClusterCoefficientBatchOp(unittest.TestCase):
    def test_vertexclustercoefficientbatchop(self):

        df = pd.DataFrame([[1, 2],
                           [1, 3],
                           [1, 4],
                           [1, 5],
                           [1, 6],
                           [2, 3],
                           [4, 3],
                           [5, 4],
                           [5, 6],
                           [5, 7],
                           [6, 7]])
        
        data = BatchOperator.fromDataframe(df, schemaStr="source double, target double")
        
        vertexClusterCoefficient = VertexClusterCoefficientBatchOp() \
            .setEdgeSourceCol("source") \
            .setEdgeTargetCol("target")
        vertexClusterCoefficient.linkFrom(data).print()
        pass