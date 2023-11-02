import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestFirstNBatchOp(unittest.TestCase):
    def test_firstnbatchop(self):

        df = pd.DataFrame([
               ["0,0,0"],
               ["0.1,0.1,0.1"],
               ["0.2,0.2,0.2"],
               ["9,9,9"],
               ["9.1,9.1,9.1"],
               ["9.2,9.2,9.2"]
        ])
        
        # batch source 
        inOp = BatchOperator.fromDataframe(df, schemaStr='Y string')
        
        sampleOp = FirstNBatchOp()\
                .setSize(2)
        
        inOp.link(sampleOp).print()
        
        pass