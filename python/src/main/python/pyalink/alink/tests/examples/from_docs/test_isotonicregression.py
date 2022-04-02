import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestIsotonicRegression(unittest.TestCase):
    def test_isotonicregression(self):

        df = pd.DataFrame([
            [0.35, 1],
            [0.6, 1],
            [0.55, 1],
            [0.5, 1],
            [0.18, 0],
            [0.1, 1],
            [0.8, 1],
            [0.45, 0],
            [0.4, 1],
            [0.7, 0],
            [0.02, 1],
            [0.3, 0],
            [0.27, 1],
            [0.2, 0],
            [0.9, 1]])
        
        data = BatchOperator.fromDataframe(df, schemaStr="label double, feature double")
        
        res = IsotonicRegression()\
                    .setFeatureCol("feature")\
                    .setLabelCol("label")\
                    .setPredictionCol("result")
        
        res.fit(data).transform(data).print()
        pass