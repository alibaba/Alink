import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestBinarySelectorPredictBatchOp(unittest.TestCase):
    def test_binaryselectorpredictbatchop(self):

        df = pd.DataFrame([
            ["$3$0:1.0 1:7.0 2:9.0", "1.0 7.0 9.0", 1.0, 7.0, 9.0, 1.0],
            ["$3$0:1.0 1:3.0 2:3.0", "2.0 3.0 3.0", 2.0, 3.0, 3.0, 1.0],
            ["$3$0:1.0 1:2.0 2:4.0", "3.0 2.0 4.0", 3.0, 2.0, 4.0, 0.0],
            ["$3$0:1.0 1:3.0 2:4.0", "2.0 3.0 4.0", 2.0, 3.0, 4.0, 0.0],
            ["$3$0:1.0 1:3.0 2:4.0", "1.0 5.0 8.0", 1.0, 5.0, 8.0, 0.0],
            ["$3$0:1.0 1:3.0 2:4.0", "1.0 6.0 3.0", 1.0, 6.0, 3.0, 0.0]
        ])
        
        batchData = BatchOperator.fromDataframe(df, schemaStr='svec string, vec string, f0 double, f1 double, f2 double, label double')
        
        selector = BinarySelectorTrainBatchOp()\
                        .setAlphaEntry(0.65)\
                        .setAlphaStay(0.7)\
                        .setSelectedCol("vec")\
                        .setLabelCol("label")\
                        .setForceSelectedCols([1])
        
        batchData.link(selector)
        
        predict = BinarySelectorPredictBatchOp()\
                    .setPredictionCol("pred")\
                    .setReservedCols(["label"])
        
        predict.linkFrom(selector, batchData).print()
        pass