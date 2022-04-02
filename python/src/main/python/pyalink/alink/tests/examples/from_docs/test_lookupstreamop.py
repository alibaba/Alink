import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLookupStreamOp(unittest.TestCase):
    def test_lookupstreamop(self):

        data_df = pd.DataFrame([
            ["10", 2.0],
            ["1", 2.0], 
            ["-3", 2.0], 
            ["5", 1.0]
        ])
        inOp = StreamOperator.fromDataframe(data_df, schemaStr='f0 string, f1 double')
        
        model_df = pd.DataFrame([
            ["1", "value1"], 
            ["2", "value2"], 
            ["5", "value5"]
        ])
        
        modelOp = BatchOperator.fromDataframe(model_df, schemaStr="key_col string, value_col string")
        
        LookupStreamOp(modelOp)\
            .setMapKeyCols(["key_col"])\
            .setMapValueCols(["value_col"]) \
            .setSelectedCols(["f0"])\
            .linkFrom(inOp)\
            .print()
        
        StreamOperator.execute()
        pass