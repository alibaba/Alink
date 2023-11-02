import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestQuantileBatchOp(unittest.TestCase):
    def test_quantilebatchop(self):

        df = pd.DataFrame([
                [1.0, 0],
                [2.0, 1],
                [3.0, 2],
                [4.0, 3]
            ])
        
        batchData = BatchOperator.fromDataframe(df, schemaStr='f0 double, f1 double')
        StreamData = StreamOperator.fromDataframe(df, schemaStr='f0 double, f1 double')
        
        QuantileBatchOp()\
            .setSelectedCol("f0")\
            .setQuantileNum(100)\
            .linkFrom(batchData)\
            .print()
            
        QuantileStreamOp()\
            .setSelectedCols(["f0"])\
            .setQuantileNum(100)\
            .linkFrom(StreamData)\
            .print()   
            
        StreamOperator.execute()
        pass