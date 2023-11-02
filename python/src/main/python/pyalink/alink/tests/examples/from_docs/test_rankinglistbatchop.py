import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestRankingListBatchOp(unittest.TestCase):
    def test_rankinglistbatchop(self):

        df = pd.DataFrame([
                ["1", "a", 1.3, 1.1],
                ["1", "b", -2.5, 0.9],
                ["2", "c", 100.2, -0.01],
                ["2", "d", -99.9, 100.9],
                ["1", "a", 1.4, 1.1],
                ["1", "b", -2.2, 0.9],
                ["2", "c", 100.9, -0.01],
                ["2", "d", -99.5, 100.9]
        ])
        
        batchData = BatchOperator.fromDataframe(df, schemaStr='id string, col1 string, col2 double, col3 double')
        
        rankList = RankingListBatchOp()\
        			.setGroupCol("id")\
        			.setGroupValues(["1", "2"])\
        			.setObjectCol("col1")\
        			.setStatCol("col2")\
        			.setStatType("sum")\
        			.setTopN(20)
                
        
        batchData.link(rankList).print()
        pass