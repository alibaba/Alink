import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestCrossFeaturePredictBatchOp(unittest.TestCase):
    def test_crossfeaturepredictbatchop(self):

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
        train = CrossFeatureTrainBatchOp().setSelectedCols(['f0','f1','f2']).linkFrom(data)
        CrossFeaturePredictBatchOp().setOutputCol("cross").linkFrom(train, data).collectToDataframe()
        pass