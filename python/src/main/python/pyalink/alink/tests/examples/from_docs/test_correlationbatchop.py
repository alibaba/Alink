import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestCorrelationBatchOp(unittest.TestCase):
    def test_correlationbatchop(self):

        df = pd.DataFrame([
                 [0.0,0.0,0.0],
                 [0.1,0.2,0.1],
                 [0.2,0.2,0.8],
                 [9.0,9.5,9.7],
                 [9.1,9.1,9.6],
                 [9.2,9.3,9.9]])
        
        source = BatchOperator.fromDataframe(df, schemaStr='x1 double, x2 double, x3 double')
        
        
        corr = CorrelationBatchOp()\
                    .setSelectedCols(["x1","x2","x3"])
        
        corr = source.link(corr).collectCorrelation()
        print(corr)
        
        pass