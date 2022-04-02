import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTsvSinkBatchOp(unittest.TestCase):
    def test_tsvsinkbatchop(self):

        df = pd.DataFrame([
                        ["0L", "1L", 0.6],
                        ["2L", "2L", 0.8],
                        ["2L", "4L", 0.6],
                        ["3L", "1L", 0.6],
                        ["3L", "2L", 0.3],
                        ["3L", "4L", 0.4]
                ])
        
        source = BatchOperator.fromDataframe(df, schemaStr='uid string, iid string, label double')
        
        filepath = '*'
        tsvSink = TsvSinkBatchOp()\
            .setFilePath(filepath)
        
        source.link(tsvSink)
        
        BatchOperator.execute()
        
        tsvSource = TsvSourceBatchOp().setFilePath(filepath).setSchemaStr("f string");
        tsvSource.print()
        
        pass