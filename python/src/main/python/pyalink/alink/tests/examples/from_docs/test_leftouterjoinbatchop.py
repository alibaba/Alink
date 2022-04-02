import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLeftOuterJoinBatchOp(unittest.TestCase):
    def test_leftouterjoinbatchop(self):

        df1 = pd.DataFrame([
            ['Ohio', 2000, 1.5],
            ['Ohio', 2001, 1.7],
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
        
        op = LeftOuterJoinBatchOp().setJoinPredicate("a.f1=b.f1").setSelectClause("a.f1, a.f2, a.f3")
        op.linkFrom(batch_data1, batch_data2).print()
        pass