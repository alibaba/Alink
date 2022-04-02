import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestChiSquareTestBatchOp(unittest.TestCase):
    def test_chisquaretestbatchop(self):

        df = pd.DataFrame([
                    ['a1','b1','c1'],
                    ['a1','b2','c1'],
                    ['a1','b1','c2'],
                    ['a2','b1','c1'],
                    ['a2','b2','c2'],
                    ['a2', 'b1','c1']
        ])
        
        batchData = BatchOperator.fromDataframe(df, schemaStr='x1 string, x2 string, x3 string')
        
        chisqTest = ChiSquareTestBatchOp()\
                    .setSelectedCols(["x1","x2"])\
                    .setLabelCol("x3")
        
        batchData.link(chisqTest).print()
        pass