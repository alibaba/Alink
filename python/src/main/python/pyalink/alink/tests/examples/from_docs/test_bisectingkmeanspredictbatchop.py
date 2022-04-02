import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestBisectingKMeansPredictBatchOp(unittest.TestCase):
    def test_bisectingkmeanspredictbatchop(self):

        df = pd.DataFrame([
            [0, "0 0 0"],
            [1, "0.1,0.1,0.1"],
            [2, "0.2,0.2,0.2"],
            [3, "9 9 9"],
            [4, "9.1 9.1 9.1"],
            [5, "9.2 9.2 9.2"]
        ])
        
        inBatch = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
        inStream = StreamOperator.fromDataframe(df, schemaStr='id int, vec string')
        
        kmeansTrain = BisectingKMeansTrainBatchOp()\
            .setVectorCol("vec")\
            .setK(2)\
            .linkFrom(inBatch)
        kmeansTrain.lazyPrint(10)
        
        predictBatch = BisectingKMeansPredictBatchOp()\
            .setPredictionCol("pred")\
            .linkFrom(kmeansTrain, inBatch)
        predictBatch.print()
        
        predictStream = BisectingKMeansPredictStreamOp(kmeansTrain)\
            .setPredictionCol("pred")\
            .linkFrom(inStream)
        predictStream.print()
        
        StreamOperator.execute()
        pass