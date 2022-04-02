import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestPrintBatchOp(unittest.TestCase):
    def test_printbatchop(self):

        df = pd.DataFrame([
               ["0,0,0"],
               ["0.1,0.1,0.1"],
               ["0.2,0.2,0.2"],
               ["9,9,9"],
               ["9.1,9.1,9.1"],
               ["9.2,9.2,9.2"]
        ])
        
        # batch source 
        inOp = BatchOperator.fromDataframe(df, schemaStr='y string')
        
        inOp.print()
        
        pass