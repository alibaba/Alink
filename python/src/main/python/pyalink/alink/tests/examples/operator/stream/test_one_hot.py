import unittest

from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def test_one_hot(self):
        import numpy as np
        import pandas as pd
        data = np.array([
            ["a", 1],
            ["b", 1],
            ["c", 1],
            ["e", 2],
            ["a", 2],
            ["b", 1],
            ["c", 2],
            ["d", 2],
            [None, 1]
        ])

        # load data
        df = pd.DataFrame({"query": data[:, 0], "label": data[:, 1]})

        inOp = dataframeToOperator(df, schemaStr='query string, weight long', op_type='batch')

        # one hot train
        one_hot = OneHotTrainBatchOp().setSelectedCols(["query"])
        model = inOp.link(one_hot)
        model.print()

        # batch predict
        predictor = OneHotPredictBatchOp().setOutputCols(["output"])
        print(BatchOperator.collectToDataframe(predictor.linkFrom(model, inOp)))
