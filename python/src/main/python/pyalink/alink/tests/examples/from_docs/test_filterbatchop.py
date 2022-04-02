import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestFilterBatchOp(unittest.TestCase):
    def test_filterbatchop(self):

        df = pd.DataFrame([
            ['Ohio', 2000, 1.5],
            ['Ohio', 2001, 1.7],
            ['Ohio', 2002, 3.6],
            ['Nevada', 2001, 2.4],
            ['Nevada', 2002, 2.9],
            ['Nevada', 2003, 3.2]
        ])
        
        batch_data = BatchOperator.fromDataframe(df, schemaStr='f1 string, f2 bigint, f3 double')
        
        op = FilterBatchOp().setClause("f1='Ohio'")
        batch_data = batch_data.link(op)
        batch_data.print()
        pass