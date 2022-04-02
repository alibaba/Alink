import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestAggLookupStreamOp(unittest.TestCase):
    def test_agglookupstreamop(self):

        data_df = pd.DataFrame([
            ["1,2,3,4", "1,2,3,4", "1,2,3,4", "1,2,3,4", "1,2,3,4"]
        ])
        
        inOp = StreamOperator.fromDataframe(data_df, schemaStr='c0 string, c1 string, c2 string, c3 string, c4 string')
        
        model_df = pd.DataFrame([
            ["1", "1.0,2.0,3.0,4.0"], 
            ["2", "2.0,3.0,4.0,5.0"], 
            ["3", "3.0,2.0,3.0,4.0"],
            ["4", "4.0,5.0,6.0,5.0"]
        ])
        modelOp = BatchOperator.fromDataframe(model_df, schemaStr="id string, vec string")
        
        AggLookupStreamOp(modelOp) \
            .setClause("CONCAT(c0,3) as e0, AVG(c1) as e1, SUM(c2) as e2,MAX(c3) as e3,MIN(c4) as e4") \
            .setDelimiter(",") \
            .setReservedCols([]) \
            .linkFrom(inOp)\
            .print()
        
        StreamOperator.execute()
        pass