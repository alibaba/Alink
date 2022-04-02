import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestSplitBatchOp(unittest.TestCase):
    def test_splitbatchop(self):

        df_data = pd.DataFrame([['Ohio', 2001, 1.7],
                                ['Ohio', 2002, 3.6],
                                ['Nevada', 2001, 2.4],
                                ['Nevada', 2002, 2.9]])
        
        batch_data = BatchOperator.fromDataframe(df_data, schemaStr='f1 string, f2 bigint, f3 double')
        
        spliter = SplitBatchOp().setFraction(0.5)
        spliter.linkFrom(batch_data)
        spliter.lazyPrint(-1)
        spliter.getSideOutput(0).lazyPrint(-1)
        
        BatchOperator.execute()
        pass