import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestEqualWidthDiscretizerPredictBatchOp(unittest.TestCase):
    def test_equalwidthdiscretizerpredictbatchop(self):

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
        
        trainOp = EqualWidthDiscretizerTrainBatchOp(). \
        setSelectedCols(['f_long', 'f_double']). \
        setNumBuckets(5). \
        linkFrom(batchSource)
        
        EqualWidthDiscretizerPredictBatchOp(). \
        setSelectedCols(['f_long', 'f_double']). \
        linkFrom(trainOp,batchSource). \
        print()
        
        trainOp = EqualWidthDiscretizerTrainBatchOp().setSelectedCols(['f_long', 'f_double']). \
        setNumBucketsArray([5,3]). \
        linkFrom(batchSource)
        
        EqualWidthDiscretizerPredictBatchOp(). \
        setSelectedCols(['f_long', 'f_double']). \
        linkFrom(trainOp,batchSource). \
        print()
        
        EqualWidthDiscretizerPredictBatchOp(). \
        setEncode("ASSEMBLED_VECTOR"). \
        setSelectedCols(['f_long', 'f_double']). \
        setOutputCols(["assVec"]). \
        linkFrom(trainOp,batchSource).print()
        pass