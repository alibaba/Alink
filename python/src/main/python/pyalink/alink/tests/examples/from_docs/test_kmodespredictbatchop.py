import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestKModesPredictBatchOp(unittest.TestCase):
    def test_kmodespredictbatchop(self):

        df = pd.DataFrame([
            ["pc", "Hp.com"],
            ["camera", "Hp.com"],
            ["digital camera", "Hp.com"],
            ["camera", "BestBuy.com"],
            ["digital camera", "BestBuy.com"],
            ["tv", "BestBuy.com"],
            ["flower", "Teleflora.com"],
            ["flower", "Orchids.com"]
        ])
        
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='f0 string, f1 string')
        inOp2 = StreamOperator.fromDataframe(df, schemaStr='f0 string, f1 string')
        
        kmodes = KModesTrainBatchOp()\
            .setFeatureCols(["f0", "f1"])\
            .setK(2)\
            .linkFrom(inOp1)
            
        predict = KModesPredictBatchOp()\
            .setPredictionCol("pred")\
            .linkFrom(kmodes, inOp1)
            
        kmodes.lazyPrint(10)
        predict.print()
        
        predict = KModesPredictStreamOp(kmodes)\
            .setPredictionCol("pred")\
            .linkFrom(inOp2)
            
        predict.print()
        
        StreamOperator.execute()
        pass