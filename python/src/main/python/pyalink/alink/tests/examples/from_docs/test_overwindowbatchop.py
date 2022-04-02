import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestOverWindowBatchOp(unittest.TestCase):
    def test_overwindowbatchop(self):

        df = pd.DataFrame([
            [2, 8, 2.2],
            [2, 7, 3.3],
            [5, 6, 4.4],
            [5, 6, 5.5],
            [7, 5, 6.6],
            [1, 8, 1.1],
            [1, 9, 1.0],
            [7, 5, 7.7],
            [9, 5, 8.8],
            [9, 4, 9.8],
            [19, 4, 8.8]])
        data = BatchOperator.fromDataframe(df, schemaStr="f0 bigint, f1 bigint, f2 double")
        OverWindowBatchOp().setOrderBy("f0, f1 desc").setClause("count_preceding(*) as cc").linkFrom(data).print()
        
        pass