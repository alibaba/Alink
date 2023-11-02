import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestConstrainedLogisticRegressionTrainBatchOp(unittest.TestCase):
    def test_constrainedlogisticregressiontrainbatchop(self):

        df = pd.DataFrame([
            [1, 1, 1, 1, 2],
            [1, 1, 0, 1, 2],
            [1, 0, 1, 1, 2],
            [1, 0, 1, 1, 2],
            [0, 1, 1, 0, 0],
            [0, 1, 1, 0, 0],
            [0, 1, 1, 0, 0],
            [0, 1, 1, 0, 0]
        ])
        
        data = BatchOperator.fromDataframe(df, schemaStr="f0 int, f1 int, f2 int, f3 int, label bigint")
        
        constraint = pd.DataFrame([
                ['{"featureConstraint":[],"constraintBetweenFeatures":{"name":"constraintBetweenFeatures","UP":[],"LO":[],"=":[["f1",0,1.814],["f0",0,0.4]],"%":[],"<":[],">":[["f3",0,"f2",0]]},"countZero":null,"elseNullSave":null}']
        ])
        constraintData = BatchOperator.fromDataframe(constraint, schemaStr='data string')
        
        features = ["f0","f1","f2","f3"]
        
        lr = ConstrainedLogisticRegressionTrainBatchOp()\
            .setLabelCol("label")\
            .setFeatureCols(features)\
            .setConstOptimMethod("sqp")\
            .setPositiveLabelValueString("2")
            
        model = lr.linkFrom(data,constraintData)
        
        predict = LogisticRegressionPredictBatchOp()\
            .setPredictionCol("lrpred")\
            .setReservedCols(["label"])
            
        predict.linkFrom(model, data).print()
        pass