import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTableSourceStreamOp(unittest.TestCase):
    def test_tablesourcestreamop(self):

        df_data = pd.DataFrame([
            [0, "0 0 0"],
            [1, "1 1 1"],
            [2, "2 2 2"]
        ])
        inOp = StreamOperator.fromDataframe(df_data, schemaStr='id int, vec string')
        TableSourceStreamOp(inOp.getOutputTable()).print()
        StreamOperator.execute()
        pass