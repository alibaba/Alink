import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestFmItemsPerUserRecommender(unittest.TestCase):
    def test_fmitemsperuserrecommender(self):

        df_data = pd.DataFrame([
            [1, 1, 0.6],
            [2, 2, 0.8],
            [2, 3, 0.6],
            [4, 1, 0.6],
            [4, 2, 0.3],
            [4, 3, 0.4],
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr='user bigint, item bigint, rating double')
        
        model = FmRecommTrainBatchOp()\
            .setUserCol("user")\
            .setItemCol("item")\
            .setNumFactor(20)\
            .setRateCol("rating").linkFrom(data);
        
        rec = FmItemsPerUserRecommender()\
            .setUserCol("user")\
            .setK(1).setReservedCols(["user"])\
            .setRecommCol("prediction_result")\
            .setModelData(model);
        
        rec.transform(data).print()
        pass