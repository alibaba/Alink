import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestBucketizerStreamOp(unittest.TestCase):
    def test_bucketizerstreamop(self):

        df = pd.DataFrame([
            [1.1, True, "2", "A"],
            [1.1, False, "2", "B"],
            [1.1, True, "1", "B"],
            [2.2, True, "1", "A"]
        ])
        
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')
        inOp2 = StreamOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')
        
        bucketizer = BucketizerBatchOp().setSelectedCols(["double"]).setCutsArray([[2.0]])
        bucketizer.linkFrom(inOp1).print()
        
        bucketizer = BucketizerStreamOp().setSelectedCols(["double"]).setCutsArray([[2.0]])
        bucketizer.linkFrom(inOp2).print()
        
        StreamOperator.execute()
        pass