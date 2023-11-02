import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestAkSourceStreamOp(unittest.TestCase):
    def test_aksourcestreamop(self):

        df = pd.DataFrame([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 3]])
        
        
        batchData = BatchOperator.fromDataframe(df, schemaStr='f0 int, f1 int, label int')
        
        filePath = "/tmp/test_alink_file_stream_sink";
        
        # write file to local disk
        batchData.link(AkSinkBatchOp()\
        				.setFilePath(FilePath(filePath))\
        				.setOverwriteSink(True)\
        				.setNumFiles(1))
        BatchOperator.execute()
        
        # read ak file and print
        AkSourceStreamOp().setFilePath(FilePath(filePath)).print()
        StreamOperator.execute()
        pass