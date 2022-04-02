import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorToTensorStreamOp(unittest.TestCase):
    def test_vectortotensorstreamop(self):

        df_data = pd.DataFrame([
            ['0.0 0.1 1.0 1.1 2.0 2.1']
        ])
        
        stream_data = StreamOperator.fromDataframe(df_data, schemaStr = 'vec string')
        
        stream_data.link(VectorToTensorStreamOp().setSelectedCol("vec")).print()
        
        StreamOperator.execute()
        
        pass