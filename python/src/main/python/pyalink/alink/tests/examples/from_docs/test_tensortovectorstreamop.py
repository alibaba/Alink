import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTensorToVectorStreamOp(unittest.TestCase):
    def test_tensortovectorstreamop(self):

        df_data = pd.DataFrame([
            ['DOUBLE#6#0.0 0.1 1.0 1.1 2.0 2.1']
        ])
        
        stream_data = StreamOperator.fromDataframe(df_data, schemaStr = 'tensor string')
        
        stream_data.link(TensorToVectorStreamOp().setSelectedCol("tensor")).print()
        
        StreamOperator.execute()
        
        pass