import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLineBatchOp(unittest.TestCase):
    def test_linebatchop(self):

        df = pd.DataFrame([\
        ["1L", "5L", 1.],\
        ["2L", "5L", 1.],\
        ["3L", "5L", 1.],\
        ["4L", "5L", 1.],\
        ["1L", "6L", 1.],\
        ["2L", "6L", 1.],\
        ["3L", "6L", 1.],\
        ["4L", "6L", 1.],\
        ["7L", "6L", 15.],\
        ["7L", "8L", 1.],\
        ["7L", "9L", 1.],\
        ["7L", "10L", 1.]])
        
        data = BatchOperator.fromDataframe(df, schemaStr="source string, target string, weight double")
        line = LineBatchOp()\
            .setOrder("firstorder")\
            .setRho(.025)\
            .setVectorSize(5)\
            .setNegative(5)\
            .setIsToUndigraph(False)\
            .setMaxIter(20)\
            .setSampleRatioPerPartition(2.)\
            .setSourceCol("source")\
            .setTargetCol("target")\
            .setWeightCol("weight")
        line.linkFrom(data).print()
        pass