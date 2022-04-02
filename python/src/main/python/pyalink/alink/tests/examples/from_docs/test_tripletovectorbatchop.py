import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTripleToVectorBatchOp(unittest.TestCase):
    def test_tripletovectorbatchop(self):

        df = pd.DataFrame([
            [1,1,1.0],
            [1,2,2.0],
            [2,1,4.0],
            [2,2,8.0]])
        
        data = BatchOperator.fromDataframe(df, schemaStr="row double, col string, val double")
        
        op = TripleToVectorBatchOp()\
            .setTripleRowCol("row")\
            .setTripleColumnCol("col")\
            .setTripleValueCol("val")\
            .setVectorCol("vec")\
            .setVectorSize(5)\
            .linkFrom(data)
        
        op.print()
        pass