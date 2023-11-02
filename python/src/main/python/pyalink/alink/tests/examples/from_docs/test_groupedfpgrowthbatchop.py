import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestGroupedFpGrowthBatchOp(unittest.TestCase):
    def test_groupedfpgrowthbatchop(self):

        df = pd.DataFrame([
            ["changjiang", "A,B,C,D"],
            ["changjiang", "B,C,E"],
            ["huanghe", "A,B,C,E"],
            ["huanghe", "B,D,E"],
            ["huanghe", "A,B,C,D"],
        ])
        
        data = BatchOperator.fromDataframe(df, schemaStr='group string, items string')
        
        fpGrowth = GroupedFpGrowthBatchOp() \
            .setItemsCol("items").setGroupCol("group").setMinSupportCount(2)
        
        fpGrowth.linkFrom(data)
        
        fpGrowth.print()
        pass