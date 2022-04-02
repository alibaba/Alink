import unittest

import pandas as pd

from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def test_glr(self):
        import numpy as np

        # data
        data = np.array([
            [1.6094, 118.0000, 69.0000, 1.0000, 2.0000],
            [2.3026, 58.0000, 35.0000, 1.0000, 2.0000],
            [2.7081, 42.0000, 26.0000, 1.0000, 2.0000],
            [2.9957, 35.0000, 21.0000, 1.0000, 2.0000],
            [3.4012, 27.0000, 18.0000, 1.0000, 2.0000],
            [3.6889, 25.0000, 16.0000, 1.0000, 2.0000],
            [4.0943, 21.0000, 13.0000, 1.0000, 2.0000],
            [4.3820, 19.0000, 12.0000, 1.0000, 2.0000],
            [4.6052, 18.0000, 12.0000, 1.0000, 2.0000]
        ])

        df = pd.DataFrame(
            {"u": data[:, 0], "lot1": data[:, 1], "lot2": data[:, 2], "offset": data[:, 3], "weights": data[:, 4]})
        source = dataframeToOperator(df, schemaStr='u double, lot1 double, lot2 double, offset double, weights double',
                                     op_type='batch')

        featureColNames = ["lot1", "lot2"]
        labelColName = "u"

        # train
        glm = GeneralizedLinearRegression() \
            .setFamily("gamma") \
            .setLink("Log") \
            .setRegParam(0.3) \
            .setMaxIter(5) \
            .setFeatureCols(featureColNames) \
            .setLabelCol(labelColName) \
            .setPredictionCol("pred")

        model = glm.fit(source)
        # predict = model.transform(source)
        eval2 = model.evaluate(source)

        # predict.print()
        eval2.print()
