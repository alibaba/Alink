import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLibSvmSourceBatchOp(unittest.TestCase):
    def test_libsvmsourcebatchop(self):

        df_data = pd.DataFrame([
            ['1:2.0 2:1.0 4:0.5', 1.5],
            ['1:2.0 2:1.0 4:0.5', 1.7],
            ['1:2.0 2:1.0 4:0.5', 3.6]
        ])
         
        batch_data = BatchOperator.fromDataframe(df_data, schemaStr='f1 string, f2  double')
        
        filepath = '/tmp/abc.svm'
        
        sink = LibSvmSinkBatchOp().setFilePath(filepath).setLabelCol("f2").setVectorCol("f1").setOverwriteSink(True)
        batch_data = batch_data.link(sink)
        
        BatchOperator.execute()
        
        batch_data = LibSvmSourceBatchOp().setFilePath(filepath)
        batch_data.print()
        
        pass