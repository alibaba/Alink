import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestSampleBatchOp(unittest.TestCase):
    def test_samplebatchop(self):

        df = pd.DataFrame([
               ["0,0,0"],
               ["0.1,0.1,0.1"],
               ["0.2,0.2,0.2"],
               ["9,9,9"],
               ["9.1,9.1,9.1"],
               ["9.2,9.2,9.2"]
        ])
             
        inOp = BatchOperator.fromDataframe(df, schemaStr='Y string')
        
        sampleOp = SampleBatchOp()\
                .setRatio(0.3)\
                .setWithReplacement(False)
        
        inOp.link(sampleOp).print()
        pass