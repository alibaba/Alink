import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestCommonNeighborsBatchOp(unittest.TestCase):
    def test_commonneighborsbatchop(self):

        df = pd.DataFrame([["a1", "11L"],\
                ["a1", "12L"],\
                ["a1", "16L"],\
                ["a2", "11L"],\
                ["a2", "12L"],\
                ["a3", "12L"],\
                ["a3", "13L"]])
        
        data = BatchOperator.fromDataframe(df, schemaStr="source string, target string")
        CommonNeighborsBatchOp()\
            .setEdgeSourceCol("source")\
            .setEdgeTargetCol("target")\
            .setIsBipartiteGraph(False)\
            .linkFrom(data)\
            .print()
        pass