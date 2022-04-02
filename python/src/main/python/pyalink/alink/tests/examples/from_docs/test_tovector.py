import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestToVector(unittest.TestCase):
    def test_tovector(self):

        df_data = pd.DataFrame([
            ['1 0 3 4']
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr = 'vec string')
        
        ToVector().setSelectedCol("vec").setVectorType("SPARSE").transform(data).print()
        pass