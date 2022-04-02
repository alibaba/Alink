import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestBucketizer(unittest.TestCase):
    def test_bucketizer(self):

        df = pd.DataFrame([
            [1.1, True, "2", "A"],
            [1.1, False, "2", "B"],
            [1.1, True, "1", "B"],
            [2.2, True, "1", "A"]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')
        bucketizer = Bucketizer().setSelectedCols(["double"]).setCutsArray([[2.0]])
        bucketizer.transform(inOp).print()
        pass