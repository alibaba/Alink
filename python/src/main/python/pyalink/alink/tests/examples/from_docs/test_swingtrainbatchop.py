import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestSwingTrainBatchOp(unittest.TestCase):
    def test_swingtrainbatchop(self):

        df_data = pd.DataFrame([
            ["a1", "11L", 2.2],
            ["a1", "12L", 2.0],
            ["a2", "11L", 2.0],
            ["a2", "12L", 2.0],
            ["a3", "12L", 2.0],
            ["a3", "13L", 2.0],
            ["a4", "13L", 2.0],
            ["a4", "14L", 2.0],
            ["a5", "14L", 2.0],
            ["a5", "15L", 2.0],
            ["a6", "15L", 2.0],
            ["a6", "16L", 2.0],
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr='user string, item string, rating double')
        
        
        model = SwingTrainBatchOp()\
            .setUserCol("user")\
            .setItemCol("item")\
            .setMinUserItems(1)\
            .linkFrom(data)
        
        model.print()
        
        predictor = SwingRecommBatchOp()\
            .setItemCol("item")\
            .setRecommCol("prediction_result")
        
        predictor.linkFrom(model, data).print()
        pass