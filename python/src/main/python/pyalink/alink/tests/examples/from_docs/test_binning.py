import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestBinning(unittest.TestCase):
    def test_binning(self):

        df = pd.DataFrame([
            [1.0, True, 0, "A", 1],
            [2.1, False, 2, "B", 1],
            [1.1, True, 3, "C", 1],
            [2.2, True, 1, "E", 0],
            [0.1, True, 2, "A", 0],
            [1.5, False, -4, "D", 1],
            [1.3, True, 1, "B", 0],
            [0.2, True, -1, "A", 1],
        ])
        
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='f0 double, f1 boolean, f2 int, f3 string, label int')
        
        binning = Binning().setEncode("INDEX").setSelectedCols(["f0", "f1", "f2", "f3"]).setLabelCol("label").setPositiveLabelValueString("1")
        binning.fit(inOp1).transform(inOp1).print()
        pass