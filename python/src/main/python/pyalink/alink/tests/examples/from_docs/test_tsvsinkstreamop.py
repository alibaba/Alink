import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTsvSinkStreamOp(unittest.TestCase):
    def test_tsvsinkstreamop(self):

        df = pd.DataFrame([
                        ["0L", "1L", 0.6],
                        ["2L", "2L", 0.8],
                        ["2L", "4L", 0.6],
                        ["3L", "1L", 0.6],
                        ["3L", "2L", 0.3],
                        ["3L", "4L", 0.4]
                ])
        
        source = StreamOperator.fromDataframe(df, schemaStr='uid string, iid string, label double')
        
        filepath = "/tmp/abc.tsv"
        tsvSink = TsvSinkStreamOp()\
            .setFilePath(filepath)\
            .setOverwriteSink(True)
        
        source.link(tsvSink)
        
        StreamOperator.execute()
        
        tsvSource = TsvSourceStreamOp().setFilePath(filepath).setSchemaStr("f string");
        tsvSource.print()
        
        StreamOperator.execute()
        
        
        pass