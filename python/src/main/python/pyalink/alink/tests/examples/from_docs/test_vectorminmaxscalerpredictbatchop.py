import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorMinMaxScalerPredictBatchOp(unittest.TestCase):
    def test_vectorminmaxscalerpredictbatchop(self):

        df = pd.DataFrame([
            ["a", "10.0, 100"],
            ["b", "-2.5, 9"],
            ["c", "100.2, 1"],
            ["d", "-99.9, 100"],
            ["a", "1.4, 1"],
            ["b", "-2.2, 9"],
            ["c", "100.9, 1"]
        ])
        
        data = BatchOperator.fromDataframe(df, schemaStr="col string, vec string")
        
        trainOp = VectorMinMaxScalerTrainBatchOp()\
                   .setSelectedCol("vec")
        model = trainOp.linkFrom(data) 
        
        batchPredictOp = VectorMinMaxScalerPredictBatchOp()
        batchPredictOp.linkFrom(model, data).print()
        pass