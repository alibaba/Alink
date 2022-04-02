import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestItemCfRateRecommender(unittest.TestCase):
    def test_itemcfraterecommender(self):

        df_data = pd.DataFrame([
            [1, 1, 0.6],
            [2, 2, 0.8],
            [2, 3, 0.6],
            [4, 1, 0.6],
            [4, 2, 0.3],
            [4, 3, 0.4],
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr='user bigint, item bigint, rating double')
        sdata = StreamOperator.fromDataframe(df_data, schemaStr='user bigint, item bigint, rating double')
        
        model = ItemCfTrainBatchOp()\
            .setUserCol("user")\
            .setItemCol("item")\
            .setRateCol("rating").linkFrom(data);
        
        predictor = ItemCfRateRecommender()\
            .setUserCol("user")\
            .setItemCol("item")\
            .setRecommCol("prediction_result")\
            .setModelData(model)
        
        predictor.transform(sdata).print()
        StreamOperator.execute()
        pass