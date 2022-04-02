import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestAppendIdBatchOp(unittest.TestCase):
    def test_appendidbatchop(self):

        df = pd.DataFrame([
            [1.0, "A", 0, 0, 0],
            [2.0, "B", 1, 1, 0],
            [3.0, "C", 2, 2, 1],
            [4.0, "D", 3, 3, 1]
        ])
        inOp = BatchOperator.fromDataframe(df, schemaStr='f0 double,f1 string,f2 int,f3 int,label int')
        AppendIdBatchOp()\
        .setIdCol("append_id")\
        .linkFrom(inOp)\
        .print()
        
        pass