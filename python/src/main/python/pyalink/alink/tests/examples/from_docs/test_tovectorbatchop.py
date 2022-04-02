import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestToVectorBatchOp(unittest.TestCase):
    def test_tovectorbatchop(self):

        df_data = pd.DataFrame([
            ['1 0 3 4']
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr = 'vec string')
        
        ToVectorBatchOp().setSelectedCol("vec").setVectorType("SPARSE").linkFrom(data).print()
        
        pass