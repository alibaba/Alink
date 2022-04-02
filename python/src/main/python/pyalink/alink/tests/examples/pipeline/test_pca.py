import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def test_pca(self):
        data = np.array([
            ["1.0 2.0 4.0", "a"],
            ["-1.0 -3.0 4.0", "a"],
            ["4.0 2.0 3.0", "b"],
            ["3.4 5.1 5.0", "b"]
        ])
        df = pd.DataFrame({"vec": data[:, 0], "lable": data[:, 1]})

        source = dataframeToOperator(df, schemaStr='vec string, label string', op_type='batch')

        pca = PCA() \
            .setK(2) \
            .setCalculationType("CORR") \
            .setPredictionCol("pred") \
            .setReservedCols(["label"]) \
            .setVectorCol("vec")

        model = pca.fit(source)
        model.transform(source).print()

    def test_pca2(self):
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

        pca = PCA() \
            .setK(2) \
            .setSelectedCols(["x1", "x2", "x3"]) \
            .setPredictionCol("pred")

        # train
        model = pca.fit(inOp)

        # batch predict
        model.transform(inOp).print()

        # stream predict
        inStreamOp = dataframeToOperator(df, schemaStr='x1 double, x2 double, x3 double', op_type='stream')

        model.transform(inStreamOp).print()

        StreamOperator.execute()
