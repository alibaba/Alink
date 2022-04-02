import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorStandardScalerPredictStreamOp(unittest.TestCase):
    def test_vectorstandardscalerpredictstreamop(self):

        df = pd.DataFrame([
            ["a", "10.0, 100"],
            ["b", "-2.5, 9"],
            ["c", "100.2, 1"],
            ["d", "-99.9, 100"],
            ["a", "1.4, 1"],
            ["b", "-2.2, 9"],
            ["c", "100.9, 1"]
        ])
        
        data = BatchOperator.fromDataframe(df, schemaStr="col1 string, vec string")
        colnames = ["col1", "vec"]
        selectedColName = "vec"
        
        trainOp = VectorStandardScalerTrainBatchOp()\
                   .setSelectedCol(selectedColName)
        
        model = trainOp.linkFrom(data)
        
        #batch predict
        batchPredictOp = VectorStandardScalerPredictBatchOp()
        batchPredictOp.linkFrom(model, data).print()
        
        #stream predict
        streamData = StreamOperator.fromDataframe(df, schemaStr="col1 string, vec string")
        
        streamPredictOp = VectorStandardScalerPredictStreamOp(trainOp)
        streamData.link(streamPredictOp).print()
        
        StreamOperator.execute()
        pass