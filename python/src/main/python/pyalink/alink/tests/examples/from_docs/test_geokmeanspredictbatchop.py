import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestGeoKMeansPredictBatchOp(unittest.TestCase):
    def test_geokmeanspredictbatchop(self):

        df = pd.DataFrame([
            [0, 0],
            [8, 8],
            [1, 2],
            [9, 10],
            [3, 1],
            [10, 7]
        ])
        
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='f0 long, f1 long')
        inOp2 = StreamOperator.fromDataframe(df, schemaStr='f0 long, f1 long')
        
        kmeans = GeoKMeansTrainBatchOp()\
                        .setLongitudeCol("f0")\
                        .setLatitudeCol("f1")\
                        .setK(2)\
                        .linkFrom(inOp1)
        kmeans.print()
        
        predict = GeoKMeansPredictBatchOp()\
                        .setPredictionCol("pred")\
                        .linkFrom(kmeans, inOp1)
        predict.print()
        
        predict = GeoKMeansPredictStreamOp(kmeans)\
                        .setPredictionCol("pred")\
                        .linkFrom(inOp2)
        predict.print()
        StreamOperator.execute()
        pass