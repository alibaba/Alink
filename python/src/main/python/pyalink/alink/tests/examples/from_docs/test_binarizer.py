import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestBinarizer(unittest.TestCase):
    def test_binarizer(self):

        df = pd.DataFrame([
            [1.1, True, "2", "A"],
            [1.1, False, "2", "B"],
            [1.1, True, "1", "B"],
            [2.2, True, "1", "A"]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')
        binarizer = Binarizer().setSelectedCol("double").setThreshold(2.0)
        binarizer.transform(inOp).print()
        pass