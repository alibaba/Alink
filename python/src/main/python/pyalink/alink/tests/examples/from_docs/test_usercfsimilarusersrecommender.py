import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestUserCfSimilarUsersRecommender(unittest.TestCase):
    def test_usercfsimilarusersrecommender(self):

        df_data = pd.DataFrame([
            [1, 1, 0.6],
            [2, 2, 0.8],
            [2, 3, 0.6],
            [4, 1, 0.6],
            [4, 2, 0.3],
            [4, 3, 0.4],
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr='user bigint, item bigint, rating double')
        
        model = UserCfTrainBatchOp()\
            .setUserCol("user")\
            .setItemCol("item")\
            .setRateCol("rating").linkFrom(data);
        
        predictor = UserCfSimilarUsersRecommender()\
            .setUserCol("user")\
            .setReservedCols(["user"])\
            .setK(1)\
            .setRecommCol("prediction_result")\
            .setModelData(model)
        
        predictor.transform(data).print()
        pass