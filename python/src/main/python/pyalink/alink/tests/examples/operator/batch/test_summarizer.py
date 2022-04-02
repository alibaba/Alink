import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def test_summarizer_op(self):
        data = np.array([
            ["a", 1, 1, 2.0, True],
            [None, 2, 2, -3.0, True],
            ["c", None, None, 2.0, False],
            ["a", 0, 0, None, None],
        ])
        df = pd.DataFrame({"f_string": data[:, 0], "f_long": data[:, 1], "f_int": data[:, 2], "f_double": data[:, 3],
                           "f_boolean": data[:, 4]})
        source = BatchOperator\
            .fromDataframe(df, schemaStr='f_string string, f_long long, f_int int, f_double double, f_boolean boolean')

        summarizer = SummarizerBatchOp() \
            .setSelectedCols(["f_double", "f_int"]) \
            .linkFrom(source)

        def post_process(d):
            self.assertEqual(type(d), TableSummary)
            self.assertEqual(len(d.getColNames()), 2)
            self.assertEqual(d.count(), 4)
            self.assertAlmostEquals(d.numMissingValue("f_double"), 1, delta=1e-9)
            self.assertAlmostEquals(d.numValidValue("f_double"), 3, delta=1e-9)
            self.assertAlmostEquals(d.max("f_double"), 2.0, delta=1e-9)
            self.assertAlmostEquals(d.min("f_int"), 0.0, delta=1e-9)
            self.assertAlmostEquals(d.mean("f_double"), 0.3333333333333333, delta=1e-9)
            self.assertAlmostEquals(d.variance("f_double"), 8.333333333333334, delta=1e-9)
            self.assertAlmostEquals(d.standardDeviation("f_double"), 2.886751345948129, delta=1e-9)
            self.assertAlmostEquals(d.normL1("f_double"), 7.0, delta=1e-9)
            self.assertAlmostEquals(d.normL2("f_double"), 4.123105625617661, delta=1e-9)

        summarizer.lazyCollectSummary(post_process)
        srt = summarizer.collectSummary()
        post_process(srt)
