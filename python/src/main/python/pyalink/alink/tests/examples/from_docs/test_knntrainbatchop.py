import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestKnnTrainBatchOp(unittest.TestCase):
    def test_knntrainbatchop(self):

        df = pd.DataFrame([
            [1, 0, 0],
            [2, 8, 8],
            [1, 1, 2],
            [2, 9, 10],
            [1, 3, 1],
            [2, 10, 7]
        ])
        
        dataSourceOp = BatchOperator.fromDataframe(df, schemaStr="label int, f0 int, f1 int")
        
        trainOp = KnnTrainBatchOp().setFeatureCols(["f0", "f1"]).setLabelCol("label").setDistanceType("EUCLIDEAN").linkFrom(dataSourceOp)
        predictOp = KnnPredictBatchOp().setPredictionCol("pred").setK(4).linkFrom(trainOp, dataSourceOp)
        predictOp.print()
        
        trainOp = KnnTrainBatchOp().setFeatureCols(["f0", "f1"]).setLabelCol("label").setDistanceType("EUCLIDEAN").linkFrom(dataSourceOp)
        predictOp = KnnPredictBatchOp().setPredictionCol("pred").setK(4).linkFrom(trainOp, dataSourceOp)
        predictOp.print()
        pass