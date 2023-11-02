import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestKCoreBatchOp(unittest.TestCase):
    def test_kcorebatchop(self):

        df = pd.DataFrame([[1, 2],\
                [1, 3],\
                [1, 4],\
                [2, 3],\
                [2, 4],\
                [3, 4],\
                [3, 5],\
                [3, 6],\
                [5, 6]])
        
        data = BatchOperator.fromDataframe(df, schemaStr="source int, target int")
        KCoreBatchOp()\
            .setEdgeSourceCol("source")\
            .setEdgeTargetCol("target")\
            .setK(2)\
            .linkFrom(data)\
            .print()
        pass