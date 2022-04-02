import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLookupBatchOp(unittest.TestCase):
    def test_lookupbatchop(self):

        df = pd.DataFrame([
            ["10", 2.0], ["1", 2.0], ["-3", 2.0], ["5", 1.0]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='f0 string, f1 double')
        
        df2 = pd.DataFrame([
            ["1", "value1"], ["2", "value2"], ["5", "value5"]
        ])
        modelOp = BatchOperator.fromDataframe(df2, schemaStr="key_col string, value_col string")
        
        LookupBatchOp().setMapKeyCols(["key_col"]).setMapValueCols(["value_col"]) \
            .setSelectedCols(["f0"]).linkFrom(modelOp, inOp).print()
        pass