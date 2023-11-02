import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestScorecardPredictBatchOp(unittest.TestCase):
    def test_scorecardpredictbatchop(self):

        df = pd.DataFrame([
            [1.0, True, 0, "A", 1],
            [2.1, False, 2, "B", 1],
            [1.1, True, 3, "C", 1],
            [2.2, True, 1, "E", 0],
            [0.1, True, 2, "A", 0],
            [1.5, False, -4, "D", 1],
            [1.3, True, 1, "B", 0],
            [0.2, True, -1, "A", 1],
        ])
        
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='f0 double, f1 boolean, f2 int, f3 string, label int')
        inOp2 = StreamOperator.fromDataframe(df, schemaStr='f0 double, f1 boolean, f2 int, f3 string, label int')
        
        binning = BinningTrainBatchOp()\
            .setSelectedCols(["f0", "f1", "f2", "f3"])\
            .setLabelCol("label")\
            .setPositiveLabelValueString("1")\
            .linkFrom(inOp1)
            
        scorecard = ScorecardTrainBatchOp()\
            .setPositiveLabelValueString("0")\
            .setSelectedCols(["f0", "f1", "f2", "f3"])\
            .setLabelCol("label")\
            .linkFrom(inOp1, binning)
            
        predict = ScorecardPredictBatchOp()\
            .setPredictionScoreCol("score")\
            .setPredictionDetailCol("detail")\
            .linkFrom(scorecard, inOp1)
            
        predict.lazyPrint(10)
        scorecard.print()
        
        predict = ScorecardPredictStreamOp(scorecard)\
            .setPredictionDetailCol("detail")\
            .setPredictionScoreCol("score")\
            .linkFrom(inOp2)
            
        predict.print()
        StreamOperator.execute()
        pass