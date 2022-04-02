import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestNode2VecWalkBatchOp(unittest.TestCase):
    def test_node2vecwalkbatchop(self):

        df = pd.DataFrame([
            [1, 1, 1.0],
            [1, 2, 1.0],
            [2, 3, 1.0],
            [3, 4, 1.0],
            [4, 2, 1.0],
            [3, 1, 1.0],
            [2, 4, 1.0],
            [4, 1, 1.0]])
        
        source = BatchOperator.fromDataframe(df, schemaStr="start int, dest int, weight double")
        
        n2vWalkBatchOp = Node2VecWalkBatchOp() \
                        .setWalkNum(4) \
                        .setWalkLength(50) \
                        .setDelimiter(",") \
                        .setSourceCol("start") \
                        .setTargetCol("dest") \
                        .setIsToUndigraph(True) \
                        .setWeightCol("weight")
        
        n2vWalkBatchOp.linkFrom(source).print()
        pass