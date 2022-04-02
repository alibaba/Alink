import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLibSvmSinkStreamOp(unittest.TestCase):
    def test_libsvmsinkstreamop(self):

        df_data = pd.DataFrame([
            ['1:2.0 2:1.0 4:0.5', 1.5],
            ['1:2.0 2:1.0 4:0.5', 1.7],
            ['1:2.0 2:1.0 4:0.5', 3.6]
        ])
         
        stream_data = StreamOperator.fromDataframe(df_data, schemaStr='f1 string, f2  double')
        
        sink = LibSvmSinkStreamOp().setFilePath('/tmp/abc.svm').setLabelCol("f2").setVectorCol("f1").setOverwriteSink(True)
        stream_data = stream_data.link(sink)
        
        StreamOperator.execute()
        
        pass