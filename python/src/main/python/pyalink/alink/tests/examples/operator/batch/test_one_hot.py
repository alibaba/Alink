import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def test_one_hot(self):
        data = np.array([
            ["assisbragasm", 1],
            ["assiseduc", 1],
            ["assist", 1],
            ["assiseduc", 1],
            ["assistebrasil", 1],
            ["assiseduc", 1],
            ["assistebrasil", 1],
            ["assistencialgsamsung", 1]
        ])

        # load data
        df = pd.DataFrame({"query": data[:, 0], "weight": data[:, 1]})

        inOp = dataframeToOperator(df, schemaStr='query string, weight long', op_type='batch')

        # one hot train
        one_hot = OneHotTrainBatchOp().setSelectedCols(["query"])
        model = inOp.link(one_hot)

        from pyalink.alink.common.types.model_info import OneHotModelInfo

        def model_info_callback(d: OneHotModelInfo):
            self.assertEquals(type(d), OneHotModelInfo)
            print("selected cols:", d.getSelectedColsInModel())
            print("category size:", d.getDistinctTokenNumber("query"))
        one_hot.lazyCollectModelInfo(model_info_callback)

        # batch predict
        predictor = OneHotPredictBatchOp().setOutputCols(["predicted_r"]).setReservedCols(["weight"]).setDropLast(False)
        print(BatchOperator.collectToDataframe(predictor.linkFrom(model, inOp)))
