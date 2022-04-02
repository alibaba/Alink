import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorToTensorBatchOp(unittest.TestCase):
    def test_vectortotensorbatchop(self):

        df_data = pd.DataFrame([
            ['0.0 0.1 1.0 1.1 2.0 2.1']
        ])
        
        batch_data = BatchOperator.fromDataframe(df_data, schemaStr = 'vec string')
        
        batch_data.link(VectorToTensorBatchOp().setSelectedCol("vec")).print()
        
        pass