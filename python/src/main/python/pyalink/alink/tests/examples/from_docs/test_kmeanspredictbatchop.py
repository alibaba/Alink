import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestKMeansPredictBatchOp(unittest.TestCase):
    def test_kmeanspredictbatchop(self):

        df = pd.DataFrame([
            [0, "0 0 0"],
            [1, "0.1,0.1,0.1"],
            [2, "0.2,0.2,0.2"],
            [3, "9 9 9"],
            [4, "9.1 9.1 9.1"],
            [5, "9.2 9.2 9.2"]
        ])
        
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
        inOp2 = StreamOperator.fromDataframe(df, schemaStr='id int, vec string')
        
        kmeans = KMeansTrainBatchOp()\
            .setVectorCol("vec")\
            .setK(2)\
            .linkFrom(inOp1)
        kmeans.lazyPrint(10)
        
        predictBatch = KMeansPredictBatchOp()\
            .setPredictionCol("pred")\
            .linkFrom(kmeans, inOp1)
        predictBatch.print()
        
        predictStream = KMeansPredictStreamOp(kmeans)\
            .setPredictionCol("pred")\
            .linkFrom(inOp2)
        predictStream.print()
        
        StreamOperator.execute()
        pass