import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestEqualWidthDiscretizer(unittest.TestCase):
    def test_equalwidthdiscretizer(self):

        df = pd.DataFrame([
            ["a", 1, 1.1],     
            ["b", -2, 0.9],    
            ["c", 100, -0.01], 
            ["d", -99, 100.9], 
            ["a", 1, 1.1],     
            ["b", -2, 0.9],    
            ["c", 100, -0.01], 
            ["d", -99, 100.9] 
        ])
        
        batchSource =  BatchOperator.fromDataframe(df,schemaStr="f_string string, f_long long, f_double double")
        streamSource = StreamOperator.fromDataframe(df,schemaStr="f_string string, f_long long, f_double double")
        
        EqualWidthDiscretizer().setSelectedCols(['f_long', 'f_double']).setNumBuckets(5).fit(batchSource).transform(batchSource).print()
        pass