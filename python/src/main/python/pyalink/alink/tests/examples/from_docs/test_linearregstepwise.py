import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLinearRegStepwise(unittest.TestCase):
    def test_linearregstepwise(self):

        df = pd.DataFrame([
            [16.3, 1.1, 1.1],
            [16.8, 1.4, 1.5],
            [19.2, 1.7, 1.8],
            [18.0, 1.7, 1.7],
            [19.5, 1.8, 1.9],
            [20.9, 1.8, 1.8],
            [21.1, 1.9, 1.8],
            [20.9, 2.0, 2.1],
            [20.3, 2.3, 2.4],
            [22.0, 2.4, 2.5]
        ])
        
        # load data
        batchData = BatchOperator.fromDataframe(df, schemaStr='y double, x1 double, x2 double')
        
        colnames = ["x1", "x2"]
        
        step = LinearRegStepwise()\
                .setFeatureCols(colnames)\
                .setLabelCol("y")\
                .setPredictionCol("pred")
        
        model = step.fit(batchData)
        model.transform(batchData).print()
        pass