import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestAls(unittest.TestCase):
    def test_predict(self):
        data = np.array([
            [1, 1, 0.6],
            [2, 2, 0.8],
            [2, 3, 0.6],
            [4, 1, 0.6],
            [4, 2, 0.3],
            [4, 3, 0.4],
        ])

        df_data = pd.DataFrame({
            "user": data[:, 0],
            "item": data[:, 1],
            "rating": data[:, 2],
        })
        df_data["user"] = df_data["user"].astype('int')
        df_data["item"] = df_data["item"].astype('int')

        data = BatchOperator.fromDataframe(df_data, schemaStr='user bigint, item bigint, rating double')

        als = AlsTrainBatchOp().setUserCol("user").setItemCol("item").setRateCol("rating") \
            .setNumIter(10).setRank(10).setLambda(0.01)
        predictor = AlsRateRecommBatchOp() \
            .setUserCol("user").setItemCol("item").setRecommCol("predicted_rating")

        model = als.linkFrom(data)
        predictor.linkFrom(model, data).print()

    def test_train(self):
        data = np.array([
            [1, 1, 0.6],
            [2, 2, 0.8],
            [2, 3, 0.6],
            [4, 1, 0.6],
            [4, 2, 0.3],
            [4, 3, 0.4],
        ])

        df_data = pd.DataFrame({
            "user": data[:, 0],
            "item": data[:, 1],
            "rating": data[:, 2],
        })
        df_data["user"] = df_data["user"].astype('int')
        df_data["item"] = df_data["item"].astype('int')

        data = dataframeToOperator(df_data, schemaStr='user bigint, item bigint, rating double',
                                   op_type='batch')

        als = AlsTrainBatchOp().setUserCol("user").setItemCol("item").setRateCol("rating") \
            .setNumIter(10).setRank(10).setLambda(0.01)

        model = als.linkFrom(data)
        model.print()
