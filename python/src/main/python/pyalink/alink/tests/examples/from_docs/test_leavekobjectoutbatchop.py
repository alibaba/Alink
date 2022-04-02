import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLeaveKObjectOutBatchOp(unittest.TestCase):
    def test_leavekobjectoutbatchop(self):

        df_data = pd.DataFrame([
            [1, 1, 0.6],
            [2, 2, 0.8],
            [2, 3, 0.6],
            [4, 0, 0.6],
            [6, 4, 0.3],
            [4, 7, 0.4],
            [2, 6, 0.6],
            [4, 5, 0.6],
            [4, 6, 0.3],
            [4, 3, 0.4]
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr='user bigint, item bigint, rating double')
        
        spliter = LeaveKObjectOutBatchOp()\
        			.setK(2)\
        			.setGroupCol("user")\
        			.setObjectCol("item")\
        			.setOutputCol("label")
        spliter.linkFrom(data).print()
        spliter.getSideOutput(0).print()
        
        pass