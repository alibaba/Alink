import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTripleToColumnsBatchOp(unittest.TestCase):
    def test_tripletocolumnsbatchop(self):

        df = pd.DataFrame([
            [1,'f1',1.0],
            [1,'f2',2.0],
            [2,'f1',4.0],
            [2,'f2',8.0]])
        
        data = BatchOperator.fromDataframe(df, schemaStr="row double, col string, val double")
        
        
        op = TripleToColumnsBatchOp()\
            .setTripleRowCol("row")\
            .setTripleColumnCol("col")\
            .setTripleValueCol("val")\
            .setSchemaStr("f1 double, f2 double")\
            .linkFrom(data)
        
        op.print()
        pass