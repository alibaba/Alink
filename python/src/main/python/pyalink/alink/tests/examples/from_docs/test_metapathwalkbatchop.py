import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestMetaPathWalkBatchOp(unittest.TestCase):
    def test_metapathwalkbatchop(self):

        df = pd.DataFrame([
            [1, 1, 1.0],
            [1, 2, 1.0],
            [2, 3, 1.0],
            [3, 4, 1.0],
            [4, 2, 1.0],
            [3, 1, 1.0],
            [2, 4, 1.0],
            [4, 1, 1.0]])
        
        graph = BatchOperator.fromDataframe(df, schemaStr="start int, dest int, weight double")
        
        df2 = pd.DataFrame([
            [1,"A"],
            [2,"B"],
            [3,"A"],
            [4,"B"]])
        
        node = BatchOperator.fromDataframe(df2, schemaStr="node int, type string")
        
        MetaPathWalkBatchOp() \
        			.setWalkNum(10) \
        			.setWalkLength(20) \
        			.setSourceCol("start") \
        			.setTargetCol("dest") \
        			.setIsToUndigraph(True) \
        			.setMetaPath("ABA") \
        			.setVertexCol("node") \
        			.setWeightCol("weight") \
        			.setTypeCol("type").linkFrom(graph, node).print();
        pass