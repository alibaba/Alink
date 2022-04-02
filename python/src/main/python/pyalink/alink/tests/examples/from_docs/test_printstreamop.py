import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestPrintStreamOp(unittest.TestCase):
    def test_printstreamop(self):

        df = pd.DataFrame([
            [0, "abcde", "aabce"],
            [1, "aacedw", "aabbed"],
            [2, "cdefa", "bbcefa"],
            [3, "bdefh", "ddeac"],
            [4, "acedm", "aeefbc"]
        ])
        
        inOp = StreamOperator.fromDataframe(df, schemaStr='id long, text1 string, text2 string')
        
        inOp.print()
        
        StreamOperator.execute()
        
        pass