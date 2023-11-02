import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestAkSourceBatchOp(unittest.TestCase):
    def test_aksourcebatchop(self):

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
        
        filePath = "/tmp/test_alink_file_sink";
        
        # write file to local disk
        batchData.link(AkSinkBatchOp()\
        				.setFilePath(FilePath(filePath))\
        				.setOverwriteSink(True)\
        				.setNumFiles(1))
        BatchOperator.execute()
        
        # read ak file and print
        AkSourceBatchOp().setFilePath(FilePath(filePath)).print()
        pass