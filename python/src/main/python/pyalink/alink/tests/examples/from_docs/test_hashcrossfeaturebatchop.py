import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestHashCrossFeatureBatchOp(unittest.TestCase):
    def test_hashcrossfeaturebatchop(self):

        df = pd.DataFrame([
            ["1.0", "1.0", 1.0, 1],
            ["1.0", "1.0", 0.0, 1],
            ["1.0", "0.0", 1.0, 1],
            ["1.0", "0.0", 1.0, 1],
            ["2.0", "3.0", None, 0],
            ["2.0", "3.0", 1.0, 0],
            ["0.0", "1.0", 2.0, 0],
            ["0.0", "1.0", 1.0, 0]])
        data = BatchOperator.fromDataframe(df, schemaStr="f0 string, f1 string, f2 double, label bigint")
        cross = HashCrossFeatureBatchOp().setSelectedCols(['f0', 'f1', 'f2']).setOutputCol('cross').setNumFeatures(4)
        print(cross.linkFrom(data))
        pass