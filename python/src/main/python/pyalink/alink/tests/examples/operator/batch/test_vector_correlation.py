import unittest

import numpy as np
import pandas as pd

from pyalink.alink.common.types.stat_summary import CorrelationResult
from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def test_vector_correlation(self):
        data = np.array([                
            ["1.0 2.0"],
                ["-1.0 -3.0"],
                ["4.0 2.0"],
        ])
        df = pd.DataFrame.from_records(data)
        source = BatchOperator\
            .fromDataframe(df, schemaStr='vec string')

        corr = VectorCorrelationBatchOp() \
            .setSelectedCol("vec") \
            .setMethod("pearson")
        corr.linkFrom(source)

        def post_process(d):
            self.assertEqual(type(d), CorrelationResult)
            self.assertListEqual(d.getCorrelationMatrix().getArrayCopy1D(True),
                                 [1.0, 0.802955068546966, 0.802955068546966, 1.0])
        corr.lazyCollectCorrelation(post_process)
        corrMat: CorrelationResult = corr.collectCorrelation()
        post_process(corrMat)
