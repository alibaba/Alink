import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestConstrainedLinearRegTrainBatchOp(unittest.TestCase):
    def test_constrainedlinearregtrainbatchop(self):

        df = pd.DataFrame([
            ["1.0 7.0 9.0", 16.8],
            ["1.0 3.0 3.0", 6.7],
            ["1.0 2.0 4.0", 6.9],
            ["1.0 3.0 4.0", 8.0]
        ])
        
        data = BatchOperator.fromDataframe(df, schemaStr="vec string, label double")
        
        constraint = pd.DataFrame([
                ['{"featureConstraint":[],"constraintBetweenFeatures":{"name":"constraintBetweenFeatures","UP":[],"LO":[],"=":[[1,1.814],[2,0.4]],"%":[],"<":[],">":[[1,2]]},"countZero":null,"elseNullSave":null}']
        ])
        constraintData = BatchOperator.fromDataframe(constraint, schemaStr='data string')
        
        batchOp = ConstrainedLinearRegTrainBatchOp()\
            .setWithIntercept(True)\
            .setVectorCol("vec")\
            .setConstOptimMethod("barrier")\
            .setLabelCol("label")
        
        model = batchOp.linkFrom(data, constraintData)
        
        predict = LinearRegPredictBatchOp()\
            .setPredictionCol("pred")
            
        predict.linkFrom(model, data).print()
        pass