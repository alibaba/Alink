import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestQuantileDiscretizerPredictStreamOp(unittest.TestCase):
    def test_quantilediscretizerpredictstreamop(self):

        df = pd.DataFrame([
            ["a", 1, 1, 2.0, True],
            ["c", 1, 2, -3.0, True],
            ["a", 2, 2, 2.0, False],
            ["c", 0, 0, 0.0, False]
            ])
        
        batchSource = BatchOperator.fromDataframe(df,schemaStr="f_string string, f_long long, f_int int, f_double double, f_boolean boolean" )
        streamSource = StreamOperator.fromDataframe(df,schemaStr="f_string string, f_long long, f_int int, f_double double, f_boolean boolean")
        
        trainOp = QuantileDiscretizerTrainBatchOp()\
            .setSelectedCols(['f_double'])\
            .setNumBuckets(8)\
            .linkFrom(batchSource)
        
        
        predictBatchOp = QuantileDiscretizerPredictBatchOp()\
            .setSelectedCols(['f_double'])
        
        predictBatchOp.linkFrom(trainOp,batchSource).print()
        
        
        predictStreamOp = QuantileDiscretizerPredictStreamOp(trainOp)\
            .setSelectedCols(['f_double'])
        
        predictStreamOp.linkFrom(streamSource).print()
        
        StreamOperator.execute()
        
        pass