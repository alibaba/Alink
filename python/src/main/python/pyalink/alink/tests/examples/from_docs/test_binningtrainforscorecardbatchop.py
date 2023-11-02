import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestBinningTrainForScorecardBatchOp(unittest.TestCase):
    def test_binningtrainforscorecardbatchop(self):

        df = pd.DataFrame([
            [0, 1.0, True, 0, "A", 1],
            [1, 2.1, False, 2, "B", 1],
            [2, 1.1, True, 3, "C", 1],
            [3, 2.2, True, 1, "E", 0],
            [4, 0.1, True, 2, "A", 0],
            [5, 1.5, False, -4, "D", 1],
            [6, 1.3, True, 1, "B", 0],
            [7, 0.2, True, -1, "A", 1],
        ])
        
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='id int,f0 double, f1 boolean, f2 int, f3 string, label int')
        inOp2 = StreamOperator.fromDataframe(df, schemaStr='id int,f0 double, f1 boolean, f2 int, f3 string, label int')
        
        train = BinningTrainForScorecardBatchOp()\
            .setSelectedCols(["f0", "f1", "f2", "f3"])\
            .setLabelCol("label")\
            .setPositiveLabelValueString("1")\
            .linkFrom(inOp1)
        
        predict = BinningPredictBatchOp()\
            .setSelectedCols(["f0", "f1", "f2", "f3"])\
            .setEncode("INDEX")\
            .setReservedCols(["id", "label"])\
            .linkFrom(train, inOp1)
        
        predict.lazyPrint(10)
        train.print()
        
        predict = BinningPredictStreamOp(train)\
            .setSelectedCols(["f0", "f1", "f2", "f3"])\
            .setEncode("INDEX")\
            .setReservedCols(["id", "label"])\
            .linkFrom(inOp2)
            
        predict.print()
        StreamOperator.execute()
        pass