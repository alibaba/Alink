import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorToTensor(unittest.TestCase):
    def test_vectortotensor(self):

        df_data = pd.DataFrame([
            ['0.0 0.1 1.0 1.1 2.0 2.1']
        ])
        
        batch_data = BatchOperator.fromDataframe(df_data, schemaStr = 'vec string')
        
        VectorToTensor().setSelectedCol("vec").transform(batch_data).print()
        
        pass