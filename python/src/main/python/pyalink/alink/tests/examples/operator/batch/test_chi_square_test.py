import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def test_chi_square_test_op(self):
        data = np.array([
            ["a", 1, 1.1, 1.2],
            ["b", -2, 0.9, 1.0],
            ["c", 100, -0.01, 1.0],
            ["d", -99, 100.9, 0.1],
            ["a", 1, 1.1, 1.2],
            ["b", -2, 0.9, 1.0],
            ["c", 100, -0.01, 0.2],
            ["d", -99, 100.9, 0.3],
        ])
        df = pd.DataFrame.from_records(data)
        source = BatchOperator.fromDataframe(df, schemaStr='col1 string, col2 long, col3 double, col4 double')

        test = ChiSquareTestBatchOp() \
            .setSelectedCols(["col3", "col1"]) \
            .setLabelCol("col2")
        test.linkFrom(source)

        def post_process(d: ChiSquareTestResults):
            self.assertEqual(type(d), ChiSquareTestResults)
            arr = d.getResults()
            self.assertEqual(type(arr[0]), ChiSquareTestResult)
            self.assertEqual(arr[0].getColName(), 'col3')
            self.assertEqual(arr[0].getP(), 0.004301310843500827, 10e-4)

        test.lazyCollectChiSquareTest(post_process)
        test.lazyPrintChiSquareTest()
        results = test.collectChiSquareTest()
        post_process(results)
