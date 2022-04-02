import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestRandomWalkBatchOp(unittest.TestCase):
    def test_randomwalkbatchop(self):

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
        
        RandomWalkBatchOp() \
        			.setWalkNum(5) \
        			.setWalkLength(20) \
        			.setSourceCol("start") \
        			.setTargetCol("dest") \
        			.setIsToUndigraph(True) \
        			.setWeightCol("weight").linkFrom(graph).print()
        pass