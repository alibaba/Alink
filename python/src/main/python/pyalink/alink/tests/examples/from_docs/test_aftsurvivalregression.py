import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestAftSurvivalRegression(unittest.TestCase):
    def test_aftsurvivalregression(self):

        df = pd.DataFrame([
            [1.218, 1.0, "1.560,-0.605"],
            [2.949, 0.0, "0.346,2.158"],
            [3.627, 0.0, "1.380,0.231"],
            [0.273, 1.0, "0.520,1.151"],
            [4.199, 0.0, "0.795,-0.226"]])
        
        data = BatchOperator.fromDataframe(df, schemaStr="label double, censor double, features string")
        
        reg = AftSurvivalRegression()\
                    .setVectorCol("features")\
                    .setLabelCol("label")\
                    .setCensorCol("censor")\
                    .setPredictionCol("result")
        
        pipeline = Pipeline().add(reg)
        model = pipeline.fit(data)
        
        model.save().lazyPrint(10)
        model.transform(data).print()
        pass