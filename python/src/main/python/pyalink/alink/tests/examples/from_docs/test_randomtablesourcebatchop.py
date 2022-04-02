import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestRandomTableSourceBatchOp(unittest.TestCase):
    def test_randomtablesourcebatchop(self):

        # 生成测试数据，该数据符合高斯分布
        data = RandomTableSourceBatchOp() \
                        .setNumCols(4) \
                        .setNumRows(10) \
                        .setIdCol("id") \
                        .setOutputCols(["group_id", "f0", "f1", "f2"]) \
                        .setOutputColConfs("group_id:weight_set(111.0,1.0,222.0,1.0);f0:gauss(0,2);f1:gauss(0,2);f2:gauss(0,2)")
        pass