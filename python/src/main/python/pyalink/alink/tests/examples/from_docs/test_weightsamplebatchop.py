import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestWeightSampleBatchOp(unittest.TestCase):
    def test_weightsamplebatchop(self):

        df = pd.DataFrame([
            ["a", 1.3, 1.1],
            ["b", 2.5, 0.9],
            ["c", 100.2, -0.01],
            ["d", 99.9, 100.9],
            ["e", 1.4, 1.1],
            ["f", 2.2, 0.9],
            ["g", 100.9, -0.01],
            ["j", 99.5, 100.9],
        ])
        
        
        
        # batch source
        inOp = BatchOperator.fromDataframe(df, schemaStr='id string, weight double, value double')
        sampleOp = WeightSampleBatchOp() \
          .setWeightCol("weight") \
          .setRatio(0.5) \
          .setWithReplacement(False)
        
        inOp.link(sampleOp).print()
        pass