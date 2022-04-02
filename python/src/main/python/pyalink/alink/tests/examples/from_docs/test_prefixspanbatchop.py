import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestPrefixSpanBatchOp(unittest.TestCase):
    def test_prefixspanbatchop(self):

        df = pd.DataFrame([
            ["a;a,b,c;a,c;d;c,f"],
            ["a,d;c;b,c;a,e"],
            ["e,f;a,b;d,f;c;b"],
            ["e;g;a,f;c;b;c"],
        ])
        
        data = BatchOperator.fromDataframe(df, schemaStr='sequence string')
        
        prefixSpan = PrefixSpanBatchOp() \
            .setItemsCol("sequence") \
            .setMinSupportCount(3)
        
        prefixSpan.linkFrom(data)
        
        prefixSpan.print()
        prefixSpan.getSideOutput(0).print()
        pass