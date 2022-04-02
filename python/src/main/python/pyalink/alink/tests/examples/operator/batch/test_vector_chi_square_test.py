import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def test_vector_chi_square_test_op(self):
        data = np.array([
            [7, "0.0  0.0  18.0  1.0", 1.0],
            [8, "0.0  1.0  12.0  0.0", 0.0],
            [9, "1.0  0.0  15.0  0.1", 0.0],
        ])
        df = pd.DataFrame.from_records(data)
        source = BatchOperator.fromDataframe(df, schemaStr='id long, features string, clicked double')

        test = VectorChiSquareTestBatchOp() \
            .setSelectedCol("features") \
            .setLabelCol("clicked")
        test.linkFrom(source)

        def post_process(d: ChiSquareTestResults):
            self.assertEqual(type(d), ChiSquareTestResults)
            arr = d.getResults()
            self.assertEqual(type(arr[0]), ChiSquareTestResult)
            self.assertAlmostEquals(arr[0].getP(), 0.3864762307712323, delta=1e-9)
            self.assertAlmostEquals(arr[0].getDf(), 1.0, delta=1e-9)
            self.assertAlmostEquals(arr[1].getP(), 0.3864762307712323, delta=1e-9)
            self.assertAlmostEquals(arr[2].getP(), 0.22313016014843035, delta=1e-9)
            self.assertAlmostEquals(arr[3].getP(), 0.22313016014843035, delta=1e-9)

        test.lazyCollectChiSquareTest(post_process)
        test.lazyPrintChiSquareTest()
        result = test.collectChiSquareTest()

        post_process(result)
