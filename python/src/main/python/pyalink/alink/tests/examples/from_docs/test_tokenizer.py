import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTokenizer(unittest.TestCase):
    def test_tokenizer(self):

        df = pd.DataFrame([
            [0, 'That is an English Book!'],
            [1, 'Do you like math?'],
            [2, 'Have a good day!']
        ])
        
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='id long, text string')
        
        op = Tokenizer().setSelectedCol("text")
        op.transform(inOp1).print()
        pass