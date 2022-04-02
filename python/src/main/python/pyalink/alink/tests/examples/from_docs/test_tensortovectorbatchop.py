import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTensorToVectorBatchOp(unittest.TestCase):
    def test_tensortovectorbatchop(self):

        df_data = pd.DataFrame([
            ['DOUBLE#6#0.0 0.1 1.0 1.1 2.0 2.1']
        ])
        
        batch_data = BatchOperator.fromDataframe(df_data, schemaStr = 'tensor string')
        
        batch_data.link(TensorToVectorBatchOp().setSelectedCol("tensor")).print()
        
        pass