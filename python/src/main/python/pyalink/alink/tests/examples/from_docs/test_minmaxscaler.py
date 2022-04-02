import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestMinMaxScaler(unittest.TestCase):
    def test_minmaxscaler(self):

        df = pd.DataFrame([
                    ["a", 10.0, 100],
                    ["b", -2.5, 9],
                    ["c", 100.2, 1],
                    ["d", -99.9, 100],
                    ["a", 1.4, 1],
                    ["b", -2.2, 9],
                    ["c", 100.9, 1]
        ])
                     
        colnames = ["col1", "col2", "col3"]
        selectedColNames = ["col2", "col3"]
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='col1 string, col2 double, col3 long')
        
        sinOp = StreamOperator.fromDataframe(df, schemaStr='col1 string, col2 double, col3 long')            
        
        model = MinMaxScaler()\
                   .setSelectedCols(selectedColNames)\
                   .fit(inOp)
        
        model.transform(inOp).print()
        
        model.transform(sinOp).print()
        
        StreamOperator.execute()
        pass