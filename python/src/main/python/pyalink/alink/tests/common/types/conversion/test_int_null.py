# !/usr/bin/env python
# coding: utf-8
import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def test_null_values(self):
        data = np.array([
            ["dadf", 10.9, -2, False],
            ["c", 100.9, 1, True],
            [None, None, None, None]
        ])

        df = pd.DataFrame({"col1": data[:, 0], "col2": data[:, 1], "col3": data[:, 2], "col4": data[:, 3]})
        inOp = dataframeToOperator(df, schemaStr='col1 string, col2 double, col3 long, col4 boolean', op_type='batch')
        res = inOp.collectToDataframe()
        print(res.dtypes)
        print(res)
        self.assertEqual(res.dtypes[0], np.object)
        self.assertEqual(res.dtypes[1], np.float64)
        self.assertEqual(res.dtypes[2], pd.Int64Dtype())
        self.assertEqual(res.dtypes[3], np.object)
        self.assertIsNone(res["col1"][2])
        self.assertTrue(np.isnan(res["col2"][2]))
        self.assertTrue(np.isnan(res["col3"][2]))
        self.assertIsNone(res["col4"][2])
