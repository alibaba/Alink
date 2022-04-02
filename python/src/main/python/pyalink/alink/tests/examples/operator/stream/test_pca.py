import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def test_pca_op(self):
        data = np.array([
            [0.0, 0.0, 0.0],
            [0.1, 0.2, 0.1],
            [0.2, 0.2, 0.8],
            [9.0, 9.5, 9.7],
            [9.1, 9.1, 9.6],
            [9.2, 9.3, 9.9]
        ])

        df = pd.DataFrame({"x1": data[:, 0], "x2": data[:, 1], "x3": data[:, 2]})

        # batch source
        inOp = dataframeToOperator(df, schemaStr='x1 double, x2 double, x3 double', op_type='batch')

        trainOp = PcaTrainBatchOp() \
            .setK(2) \
            .setSelectedCols(["x1", "x2", "x3"])

        predictOp = PcaPredictBatchOp() \
            .setPredictionCol("pred")

        # batch train
        inOp.link(trainOp)

        # batch predict
        predictOp.linkFrom(trainOp, inOp)

        predictOp.print()

        # stream predict
        inStreamOp = dataframeToOperator(df, schemaStr='x1 double, x2 double, x3 double', op_type='stream')

        predictStreamOp = PcaPredictStreamOp(trainOp) \
            .setPredictionCol("pred")

        predictStreamOp.linkFrom(inStreamOp)

        predictStreamOp.print()

        StreamOperator.execute()
