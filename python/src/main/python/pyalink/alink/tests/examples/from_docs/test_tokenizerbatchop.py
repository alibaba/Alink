import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTokenizerBatchOp(unittest.TestCase):
    def test_tokenizerbatchop(self):

        df = pd.DataFrame([
            [0, 'That is an English Book!'],
            [1, 'Do you like math?'],
            [2, 'Have a good day!']
        ])
        
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='id long, text string')
        
        op = TokenizerBatchOp().setSelectedCol("text")
        op.linkFrom(inOp1).print()
        
        inOp2 = StreamOperator.fromDataframe(df, schemaStr='id long, text string')
        op2 = TokenizerStreamOp().setSelectedCol("text")
        op2.linkFrom(inOp2).print()
        
        StreamOperator.execute()
        pass