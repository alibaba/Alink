import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestToMTable(unittest.TestCase):
    def test_tomtable(self):

        df_data = pd.DataFrame([
            ['{"data":{"col0":[1],"col1":["2"],"label":[0]},"schema":"col0 INT, col1 VARCHAR,label INT"}']
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr = 'vec string')
        
        ToMTable().setSelectedCol("vec").transform(data).print()
        
        pass