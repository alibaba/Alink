import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestGeoKMeans(unittest.TestCase):
    def test_geokmeans(self):

        df = pd.DataFrame([
            [0, 0],
            [8, 8],
            [1, 2],
            [9, 10],
            [3, 1],
            [10, 7]
        ])
        
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='f0 long, f1 long')
        
        kmeans = GeoKMeans()\
            .setLongitudeCol("f0")\
            .setLatitudeCol("f1")\
            .setK(2)\
            .setPredictionCol("pred")
        
        kmeans.fit(inOp1).transform(inOp1).print()
        pass