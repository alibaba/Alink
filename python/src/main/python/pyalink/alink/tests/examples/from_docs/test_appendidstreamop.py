import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestAppendIdStreamOp(unittest.TestCase):
    def test_appendidstreamop(self):

        df_data = pd.DataFrame([
            ['Ohio', 2000, 1.5],
            ['Ohio', 2001, 1.7],
            ['Ohio', 2002, 3.6],
            ['Nevada', 2001, 2.4],
            ['Nevada', 2002, 2.9],
            ['Nevada', 2003,3.2]
        ])
        
        stream_data = StreamOperator.fromDataframe(df_data, schemaStr='f1 string, f2 bigint, f3 double')
        
        AppendIdStreamOp().linkFrom(stream_data).print()
        
        StreamOperator.execute()
        pass