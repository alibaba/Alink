import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTensorToVector(unittest.TestCase):
    def test_tensortovector(self):

        df_data = pd.DataFrame([
            ['DOUBLE#6#0.0 0.1 1.0 1.1 2.0 2.1']
        ])
        
        batch_data = BatchOperator.fromDataframe(df_data, schemaStr = 'tensor string')
        
        TensorToVector().setSelectedCol("tensor").transform(batch_data).print()
        
        pass