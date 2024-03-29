import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLassoRegression(unittest.TestCase):
    def test_lassoregression(self):

        df = pd.DataFrame([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 3]])
        
        batchData = BatchOperator.fromDataframe(df, schemaStr='f0 int, f1 int, label int')
        colnames = ["f0","f1"]
        lasso = LassoRegression()\
                    .setFeatureCols(colnames)\
                    .setLambda(0.1)\
                    .setLabelCol("label")\
                    .setPredictionCol("pred")
        model = lasso.fit(batchData)
        model.transform(batchData).print()
        pass