import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestAsStreamOp(unittest.TestCase):
    def test_asstreamop(self):

        df = pd.DataFrame([
            ['Ohio', 2000, 1.5],
            ['Ohio', 2001, 1.7],
            ['Ohio', 2002, 3.6],
            ['Nevada', 2001, 2.4],
            ['Nevada', 2002, 2.9],
            ['Nevada', 2003, 3.2]
        ])
        
        stream_data = StreamOperator.fromDataframe(df, schemaStr='f1 string, f2 bigint, f3 double')
        op = AsStreamOp().setClause("ff1,ff2,ff3")
        stream_data = stream_data.link(op)
        stream_data.print()
        StreamOperator.execute()
        pass