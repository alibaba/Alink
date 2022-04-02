import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestQingzhao(unittest.TestCase):

    def run_isotonic_regression_op(self):
        data = np.array([[0.35, 1], \
                         [0.6, 1], \
                         [0.55, 1], \
                         [0.5, 1], \
                         [0.18, 0], \
                         [0.1, 1], \
                         [0.8, 1], \
                         [0.45, 0], \
                         [0.4, 1], \
                         [0.7, 0], \
                         [0.02, 1], \
                         [0.3, 0], \
                         [0.27, 1], \
                         [0.2, 0], \
                         [0.9, 1]])

        df = pd.DataFrame({"feature": data[:, 0], "label": data[:, 1]})
        data = dataframeToOperator(df, schemaStr="feature double, label double", op_type="batch")

        trainOp = IsotonicRegTrainBatchOp() \
            .setFeatureCol("feature") \
            .setLabelCol("label")
        model = trainOp.linkFrom(data).print()
