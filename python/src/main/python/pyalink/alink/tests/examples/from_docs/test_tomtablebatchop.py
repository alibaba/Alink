import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestToMTableBatchOp(unittest.TestCase):
    def test_tomtablebatchop(self):

        df_data = pd.DataFrame([
            ['{"data":{"col0":[1],"col1":["2"],"label":[0]},"schema":"col0 INT, col1 VARCHAR,label INT"}']
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr = 'vec string')
        
        ToMTableBatchOp().setSelectedCol("vec").linkFrom(data).print()
        
        pass