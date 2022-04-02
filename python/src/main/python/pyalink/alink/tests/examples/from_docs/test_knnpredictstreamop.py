import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestKnnPredictStreamOp(unittest.TestCase):
    def test_knnpredictstreamop(self):

        df = pd.DataFrame([
            [1, 0, 0],
            [2, 8, 8],
            [1, 1, 2],
            [2, 9, 10],
            [1, 3, 1],
            [2, 10, 7]
        ])
        
        dataSourceOp = BatchOperator.fromDataframe(df, schemaStr="label int, f0 int, f1 int")
        streamData = StreamOperator.fromDataframe(df, schemaStr="label int, f0 int, f1 int")
        
        knnModel = KnnTrainBatchOp().setFeatureCols(["f0", "f1"]).setLabelCol("label").setDistanceType("EUCLIDEAN").linkFrom(dataSourceOp)
        predictor = KnnPredictStreamOp(knnModel).setPredictionCol("pred").setK(4)
        predictor.linkFrom(streamData).print()
        StreamOperator.execute()
        pass