import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestHugeLookupBatchOp(unittest.TestCase):
    def test_hugelookupbatchop(self):

        data_df = pd.DataFrame([
            [10, 2.0], 
            [1, 2.0], 
            [-3, 2.0], 
            [5, 1.0]
        ])
        
        inOp = BatchOperator.fromDataframe(data_df, schemaStr='f0 int, f1 double')
        
        model_df = pd.DataFrame([
            [1, "value1"], 
            [2, "value2"], 
            [5, "value5"]
        ])
        
        modelOp = BatchOperator.fromDataframe(model_df, schemaStr="key_col int, value_col string")
        
        HugeLookupBatchOp()\
            .setMapKeyCols(["key_col"])\
            .setMapValueCols(["value_col"])\
            .setSelectedCols(["f0"])\
            .linkFrom(modelOp, inOp)\
            .print()
        pass