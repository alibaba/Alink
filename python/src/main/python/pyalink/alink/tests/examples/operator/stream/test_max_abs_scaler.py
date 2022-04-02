import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def run_max_abs_scaler_op(self):
        data = np.array([
            ["a", 10.0, 100],
            ["b", -2.5, 9],
            ["c", 100.2, 1],
            ["d", -99.9, 100],
            ["a", 1.4, 1],
            ["b", -2.2, 9],
            ["c", 100.9, 1]
        ])

        colnames = ["col1", "col2", "col3"]
        selectedColNames = ["col2", "col3"]

        df = pd.DataFrame({"col1": data[:, 0], "col2": data[:, 1], "col3": data[:, 2]})
        inOp = dataframeToOperator(df, schemaStr='col1 string, col2 double, col3 long', op_type='batch')

        # train
        trainOp = MaxAbsScalerTrainBatchOp() \
            .setSelectedCols(selectedColNames)

        trainOp.linkFrom(inOp)

        # batch predict
        predictOp = MaxAbsScalerPredictBatchOp()
        predictOp.linkFrom(trainOp, inOp).print()

        # stream predict
        sinOp = dataframeToOperator(df, schemaStr='col1 string, col2 double, col3 long', op_type='stream')

        predictStreamOp = MaxAbsScalerPredictStreamOp(trainOp)
        predictStreamOp.linkFrom(sinOp).print()

        StreamOperator.execute()
