import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestQuantileDiscretizer(unittest.TestCase):
    def test_quantilediscretizer(self):

        df = pd.DataFrame([
            ["a", 1, 1, 2.0, True],
            ["c", 1, 2, -3.0, True],
            ["a", 2, 2, 2.0, False],
            ["c", 0, 0, 0.0, False]
        ])
        
        batchSource = BatchOperator.fromDataframe(
            df, schemaStr='f_string string, f_long long, f_int int, f_double double, f_boolean boolean')
        streamSource = StreamOperator.fromDataframe(
            df, schemaStr='f_string string, f_long long, f_int int, f_double double, f_boolean boolean')
        
        QuantileDiscretizer()\
            .setSelectedCols(['f_double'])\
            .setNumBuckets(8)\
            .fit(batchSource)\
            .transform(batchSource)\
            .print()
        
        QuantileDiscretizer()\
            .setSelectedCols(['f_double'])\
            .setNumBuckets(8)\
            .fit(batchSource)\
            .transform(streamSource)\
            .print()
        
        StreamOperator.execute()
        pass