import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestToVectorStreamOp(unittest.TestCase):
    def test_tovectorstreamop(self):

        df_data = pd.DataFrame([
            ['1 0 3 4']
        ])
        
        data = StreamOperator.fromDataframe(df_data, schemaStr = 'vec string')
        
        ToVectorStreamOp().setSelectedCol("vec").setVectorType("SPARSE").linkFrom(data).print()
        StreamOperator.execute()
        pass