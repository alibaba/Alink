import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestPcaPredictBatchOp(unittest.TestCase):
    def test_pcapredictbatchop(self):

        df = pd.DataFrame([
                [0.0,0.0,0.0],
                [0.1,0.2,0.1],
                [0.2,0.2,0.8],
                [9.0,9.5,9.7],
                [9.1,9.1,9.6],
                [9.2,9.3,9.9]
        ])
        
        # batch source 
        inOp = BatchOperator.fromDataframe(df, schemaStr='x1 double, x2 double, x3 double')
        
        trainOp = PcaTrainBatchOp()\
               .setK(2)\
               .setSelectedCols(["x1","x2","x3"])
        
        predictOp = PcaPredictBatchOp()\
                .setPredictionCol("pred")
        
        # batch train
        inOp.link(trainOp)
        
        # batch predict
        predictOp.linkFrom(trainOp,inOp)
        
        predictOp.print()
        
        # stream predict
        inStreamOp = StreamOperator.fromDataframe(df, schemaStr='x1 double, x2 double, x3 double')
        
        predictStreamOp = PcaPredictStreamOp(trainOp)\
                .setPredictionCol("pred")
        
        predictStreamOp.linkFrom(inStreamOp)
        
        predictStreamOp.print()
        
        StreamOperator.execute()
        pass