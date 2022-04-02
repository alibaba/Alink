import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestWhereStreamOp(unittest.TestCase):
    def test_wherestreamop(self):

        df = pd.DataFrame([
            ["a", 1, 1.1, 1.2],
            ["b", -2, 0.9, 1.0],
            ["c", 100, -0.01, 1.0],
            ["d", -99, 100.9, 0.1],
            ["a", 1, 1.1, 1.2],
            ["b", -2, 0.9, 1.0],
            ["c", 100, -0.01, 0.2],
            ["d", -99, 100.9, 0.3]
        ])
        source = StreamOperator.fromDataframe(df, schemaStr='col1 string, col2 int, col3 double, col4 double')
        source.link(WhereStreamOp().setClause("col1='a'")).print()
        StreamOperator.execute()
        
        pass