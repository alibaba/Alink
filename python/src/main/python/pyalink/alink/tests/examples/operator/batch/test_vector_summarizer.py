import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def test_vector_summarizer_op(self):
        data = np.array([
            ["1.0 2.0"],
            ["-1.0 -3.0"],
            ["4.0 2.0"]
        ])
        df = pd.DataFrame.from_records(data, columns=["vec"])
        source = BatchOperator.fromDataframe(df, schemaStr='vec string')

        summarizer = VectorSummarizerBatchOp() \
            .setSelectedCol("vec") \
            .linkFrom(source)

        def post_process(d):
            from pyalink.alink.common.types.stat_summary import DenseVectorSummary, SparseVectorSummary
            self.assertTrue(isinstance(d, (DenseVectorSummary, SparseVectorSummary)))
            self.assertEqual(d.vectorSize(), 2)
            self.assertEqual(d.count(), 3)
            self.assertEqual(d.max(0), 4.0, 10e-4)
            self.assertEqual(d.min(0), -1.0, 10e-4)
            self.assertEqual(d.mean(0), 1.3333333333333333, 10e-4)
            self.assertEqual(d.variance(0), 6.333333333333334, 10e-4)
            self.assertEqual(d.standardDeviation(0), 2.5166114784235836, 10e-4)
            self.assertEqual(d.normL1(0), 6.0, 10e-4)
            self.assertEqual(d.normL2(0), 4.242640687119285, 10e-4)

        summarizer.lazyCollectVectorSummary(post_process).lazyPrintVectorSummary()
        srt = summarizer.collectVectorSummary()
        post_process(srt)
