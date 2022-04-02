import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestFmRecommBinaryImplicitTrainBatchOp(unittest.TestCase):
    def test_fmrecommbinaryimplicittrainbatchop(self):

        df_data = pd.DataFrame([
            [1, 1, 0.6],
            [2, 2, 0.8],
            [2, 3, 0.6],
            [4, 1, 0.6],
            [4, 2, 0.3],
            [4, 3, 0.4],
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr='user bigint, item bigint, rating double')
        
        model = FmRecommBinaryImplicitTrainBatchOp()\
            .setUserCol("user")\
            .setItemCol("item")\
            .setNumFactor(20).linkFrom(data);
        
        predictor = FmUsersPerItemRecommBatchOp()\
            .setItemCol("user")\
            .setK(2).setReservedCols(["item"])\
            .setRecommCol("prediction_result");
        
        predictor.linkFrom(model, data).print()
        pass