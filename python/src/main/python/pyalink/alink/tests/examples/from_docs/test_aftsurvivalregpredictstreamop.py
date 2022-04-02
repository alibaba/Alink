import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestAftSurvivalRegPredictStreamOp(unittest.TestCase):
    def test_aftsurvivalregpredictstreamop(self):

        df = pd.DataFrame([
            [1.218, 1.0, "1.560,-0.605"],
            [2.949, 0.0, "0.346,2.158"],
            [3.627, 0.0, "1.380,0.231"],
            [0.273, 1.0, "0.520,1.151"],
            [4.199, 0.0, "0.795,-0.226"]])
        
        data = BatchOperator.fromDataframe(df, schemaStr="label double, censor double, features string")
        dataStream = StreamOperator.fromDataframe(df, schemaStr="label double, censor double, features string")
        
        trainOp = AftSurvivalRegTrainBatchOp()\
                        .setVectorCol("features")\
                        .setLabelCol("label")\
                        .setCensorCol("censor")
        
        model = trainOp.linkFrom(data)
        
        predictOp = AftSurvivalRegPredictStreamOp(model)\
                        .setPredictionCol("pred")
        
        predictOp.linkFrom(dataStream).print()
        StreamOperator.execute()
        pass