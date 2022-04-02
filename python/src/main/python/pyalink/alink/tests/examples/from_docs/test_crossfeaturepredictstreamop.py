import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestCrossFeaturePredictStreamOp(unittest.TestCase):
    def test_crossfeaturepredictstreamop(self):

        df = pd.DataFrame([
        ["1.0", "1.0", 1.0, 1],
        ["1.0", "1.0", 0.0, 1],
        ["1.0", "0.0", 1.0, 1],
        ["1.0", "0.0", 1.0, 1],
        ["2.0", "3.0", None, 0],
        ["2.0", "3.0", 1.0, 0],
        ["0.0", "1.0", 2.0, 0],
        ["0.0", "1.0", 1.0, 0]])
        batchData = BatchOperator.fromDataframe(df, schemaStr="f0 string, f1 string, f2 double, label bigint")
        streamData = StreamOperator.fromDataframe(df, schemaStr="f0 string, f1 string, f2 double, label bigint")
        train = CrossFeatureTrainBatchOp().setSelectedCols(['f0','f1','f2']).linkFrom(batchData)
        CrossFeaturePredictStreamOp(train).setOutputCol("cross").linkFrom(streamData).print()
        StreamOperator.execute()
        pass