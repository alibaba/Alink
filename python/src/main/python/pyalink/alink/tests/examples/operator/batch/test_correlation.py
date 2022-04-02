import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def test_correlation(self):
        data = np.array([
            ["a", 1, 1, 2.0, True],
            [None, 2, 2, -3.0, True],
            ["c", None, None, 2.0, False],
            ["a", 0, 0, None, None]
        ])
        df = pd.DataFrame({"f_string": data[:, 0], "f_long": data[:, 1], "f_int": data[:, 2], "f_double": data[:, 3], "f_boolean": data[:, 4]})
        source = BatchOperator\
            .fromDataframe(df, schemaStr='f_string string, f_long long, f_int int, f_double double, f_boolean boolean')

        corr = CorrelationBatchOp() \
            .setSelectedCols(["f_double", "f_int", "f_long"]) \
            .setMethod("pearson")
        corr.linkFrom(source)

        def post_process(d):
            self.assertEqual(type(d), CorrelationResult)
            self.assertListEqual(d.getCorrelationMatrix().getArrayCopy1D(True),
                                 [1., -1., -1., -1., 1., 1., -1., 1., 1.])
            self.assertListEqual(d.getCorrelation(), [[1.0, -1.0, -1.0], [-1.0, 1.0, 1.0], [-1.0, 1.0, 1.0]])

        corr.lazyCollectCorrelation(post_process)
        corr.lazyPrintCorrelation()
        corrMat: CorrelationResult = corr.collectCorrelation()
        post_process(corrMat)
