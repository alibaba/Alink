import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestEqualWidthDiscretizerPredictStreamOp(unittest.TestCase):
    def test_equalwidthdiscretizerpredictstreamop(self):

        df = pd.DataFrame([
            ["a", 1, 1.1],     
            ["b", -2, 0.9],    
            ["c", 100, -0.01], 
            ["d", -99, 100.9], 
            ["a", 1, 1.1],     
            ["b", -2, 0.9],    
            ["c", 100, -0.01], 
            ["d", -99, 100.9] 
        ])
        
        batchSource =  BatchOperator.fromDataframe(df,schemaStr="f_string string, f_long long, f_double double")
        streamSource =  StreamOperator.fromDataframe(df,schemaStr="f_string string, f_long long, f_double double")
        
        trainOp = EqualWidthDiscretizerTrainBatchOp(). \
            setSelectedCols(['f_long', 'f_double']). \
            setNumBuckets(5). \
            linkFrom(batchSource)
        
        EqualWidthDiscretizerPredictStreamOp(trainOp). \
            setSelectedCols(['f_long', 'f_double']). \
            linkFrom(streamSource). \
            print()
        
        trainOp = EqualWidthDiscretizerTrainBatchOp().setSelectedCols(['f_long', 'f_double']). \
            setNumBucketsArray([5,3]). \
            linkFrom(batchSource)
        
        EqualWidthDiscretizerPredictStreamOp(trainOp). \
            setSelectedCols(['f_long', 'f_double']). \
            linkFrom(streamSource). \
            print()
        
        EqualWidthDiscretizerPredictStreamOp(trainOp). \
            setEncode("ASSEMBLED_VECTOR"). \
            setSelectedCols(['f_long', 'f_double']). \
            setOutputCols(["assVec"]). \
            linkFrom(streamSource).print()
        
        StreamOperator.execute()
        pass