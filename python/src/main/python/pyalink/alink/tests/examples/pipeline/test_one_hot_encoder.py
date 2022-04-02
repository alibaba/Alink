import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def test_one_hot_encoder(self):
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
        one_hot = OneHotEncoder() \
            .setSelectedCols(["query"]) \
            .setDropLast(False) \
            .setOutputCols(["predicted_r"]) \
            .setReservedCols(["weight"])

        model = one_hot.fit(inOp)
        model.transform(inOp).print()

        # stream predict
        inOp2 = dataframeToOperator(df, schemaStr='query string, weight long', op_type='stream')
        model.transform(inOp2).print()

        StreamOperator.execute()
