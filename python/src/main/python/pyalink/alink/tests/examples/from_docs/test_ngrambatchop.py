import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestNGramBatchOp(unittest.TestCase):
    def test_ngrambatchop(self):

        df = pd.DataFrame([
            [0, 'That is an English Book!'],
            [1, 'Do you like math?'],
            [2, 'Have a good day!']
        ])
        
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='id long, text string')
        
        op = NGramBatchOp().setSelectedCol("text")
        op.linkFrom(inOp1).print()
        
        inOp2 = StreamOperator.fromDataframe(df, schemaStr='id long, text string')
        op2 = NGramStreamOp().setSelectedCol("text")
        op2.linkFrom(inOp2).print()
        
        StreamOperator.execute()
        
        pass