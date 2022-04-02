import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTripleToCsvBatchOp(unittest.TestCase):
    def test_tripletocsvbatchop(self):

        df = pd.DataFrame([
            [1,'f1',1.0],
            [1,'f2',2.0],
            [2,'f1',4.0],
            [2,'f2',8.0]])
        
        data = BatchOperator.fromDataframe(df, schemaStr="row double, col string, val double")
        
        op = TripleToCsvBatchOp()\
            .setTripleRowCol("row")\
            .setTripleColumnCol("col")\
            .setTripleValueCol("val")\
            .setCsvCol("csv")\
            .setSchemaStr("f1 string, f2 string")\
            .linkFrom(data)
        
        op.print()
        pass