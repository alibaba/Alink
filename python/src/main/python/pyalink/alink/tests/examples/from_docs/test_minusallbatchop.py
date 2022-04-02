import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestMinusAllBatchOp(unittest.TestCase):
    def test_minusallbatchop(self):

        df1 = pd.DataFrame([
            ['Ohio', 2000, 1.5],
            ['Ohio', 2000, 1.5],
            ['Ohio', 2002, 3.6],
            ['Nevada', 2001, 2.4],
            ['Nevada', 2002, 2.9],
            ['Nevada', 2003, 3.2]
        ])
        df2 = pd.DataFrame([
            ['Nevada', 2001, 2.4],
            ['Nevada', 2003, 3.2]
        ])
        
        batch_data1 = BatchOperator.fromDataframe(df1, schemaStr='f1 string, f2 bigint, f3 double')
        batch_data2 = BatchOperator.fromDataframe(df2, schemaStr='f1 string, f2 bigint, f3 double')
        
        MinusAllBatchOp().linkFrom(batch_data1, batch_data2).print()
        pass