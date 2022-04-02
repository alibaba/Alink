import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTableSourceBatchOp(unittest.TestCase):
    def test_tablesourcebatchop(self):

        df = pd.DataFrame([
            [0, "0 0 0"],
            [1, "1 1 1"],
            [2, "2 2 2"]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
        inOp.getOutputTable()
        TableSourceBatchOp(inOp.getOutputTable()).print()
        pass