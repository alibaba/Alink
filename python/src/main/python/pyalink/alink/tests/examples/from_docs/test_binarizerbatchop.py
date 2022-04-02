import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestBinarizerBatchOp(unittest.TestCase):
    def test_binarizerbatchop(self):

        df = pd.DataFrame([
            [1.1, True, "2", "A"],
            [1.1, False, "2", "B"],
            [1.1, True, "1", "B"],
            [2.2, True, "1", "A"]
        ])
        
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')
        inOp2 = StreamOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')
        
        binarizer = BinarizerBatchOp().setSelectedCol("double").setThreshold(2.0)
        binarizer.linkFrom(inOp1).print()
        
        binarizer = BinarizerStreamOp().setSelectedCol("double").setThreshold(2.0)
        binarizer.linkFrom(inOp2).print()
        
        StreamOperator.execute()
        pass