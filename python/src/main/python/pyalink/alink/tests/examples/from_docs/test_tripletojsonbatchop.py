import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTripleToJsonBatchOp(unittest.TestCase):
    def test_tripletojsonbatchop(self):

        df = pd.DataFrame([
            [1,'f1',1.0],
            [1,'f2',2.0],
            [2,'f1',4.0],
            [2,'f2',8.0]])
        
        data = BatchOperator.fromDataframe(df, schemaStr="row double, col string, val double")
        
        op = TripleToJsonBatchOp()\
            .setTripleRowCol("row")\
            .setTripleColumnCol("col")\
            .setTripleValueCol("val")\
            .setJsonCol("json")\
            .linkFrom(data)
        
        op.print()
        pass