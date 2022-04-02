import unittest

from pyalink.alink import *


class TestAls(unittest.TestCase):
    def test_predict(self):
        import pandas as pd
        data = [
            (0, "a", 1.0),
            (0, "b", 3.0),
            (0, "c", 2.0),
            (1, "a", 5.0),
            (1, "b", 4.0),
            (2, "b", 1.0),
            (2, "c", 4.0),
            (2, "d", 3.0)
        ]
        df = pd.DataFrame.from_records(data)
        empty_rate = BatchOperator.fromDataframe(df, "user bigint, item string, rate double")

        splitter = LeaveTopKObjectOutBatchOp() \
            .setK(2) \
            .setObjectCol("item") \
            .setRateCol("rate") \
            .setOutputCol("label") \
            .setGroupCol("user")

        test = splitter.linkFrom(empty_rate)
        train = splitter.getSideOutput(0)

        trainBatchOp = ItemCfTrainBatchOp() \
            .setSimilarityType("PEARSON") \
            .setUserCol("user") \
            .setItemCol("item") \
            .setRateCol("rate") \
            .linkFrom(train)

        recommender = ItemCfItemsPerUserRecommender() \
            .setUserCol("user") \
            .setRecommCol("recomm") \
            .setModelData(trainBatchOp)

        model = Pipeline() \
            .add(recommender) \
            .fit(trainBatchOp)
        result = model.transform(test).collectToDataframe()
        print(result)
