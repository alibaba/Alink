import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestFpGrowthBatchOp(unittest.TestCase):
    def test_fpgrowthbatchop(self):

        df = pd.DataFrame([
            ["A,B,C,D"],
            ["B,C,E"],
            ["A,B,C,E"],
            ["B,D,E"],
            ["A,B,C,D"],
        ])
        
        data = BatchOperator.fromDataframe(df, schemaStr='items string')
        
        fpGrowth = FpGrowthBatchOp() \
            .setItemsCol("items") \
            .setMinSupportPercent(0.4) \
            .setMinConfidence(0.6)
        
        fpGrowth.linkFrom(data)
        
        fpGrowth.print()
        fpGrowth.getSideOutput(0).print()
        
        pass