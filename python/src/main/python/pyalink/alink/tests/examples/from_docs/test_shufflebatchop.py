import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestShuffleBatchOp(unittest.TestCase):
    def test_shufflebatchop(self):

        df = pd.DataFrame([
               ["0,0,0"],
               ["0.1,0.1,0.1"],
               ["0.2,0.2,0.2"],
               ["9,9,9"],
               ["9.1,9.1,9.1"],
               ["9.2,9.2,9.2"]
        ])
             
        inOp = BatchOperator.fromDataframe(df, schemaStr='Y string')
        
        inOp.link(ShuffleBatchOp()).print()
        
        pass