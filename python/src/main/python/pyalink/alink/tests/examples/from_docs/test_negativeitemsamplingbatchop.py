import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestNegativeItemSamplingBatchOp(unittest.TestCase):
    def test_negativeitemsamplingbatchop(self):

        df_data = pd.DataFrame([
             [1, 1],
             [2, 2],
             [2, 3],
             [4, 1],
             [4, 2],
             [4, 3],
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr='user bigint, item bigint')
        
        NegativeItemSamplingBatchOp().linkFrom(data).print()
        pass