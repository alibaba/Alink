import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestImputerPredictBatchOp(unittest.TestCase):
    def test_imputerpredictbatchop(self):

        df_data = pd.DataFrame([
                    ["a", 10.0, 100],
                    ["b", -2.5, 9],
                    ["c", 100.2, 1],
                    ["d", -99.9, 100],
                    ["a", 1.4, 1],
                    ["b", -2.2, 9],
                    ["c", 100.9, 1],
                    [None, None, None]
        ])
                     
        colnames = ["col1", "col2", "col3"]
        selectedColNames = ["col2", "col3"]
        
        inOp = BatchOperator.fromDataframe(df_data, schemaStr='col1 string, col2 double, col3 double')
        
        # train
        trainOp = ImputerTrainBatchOp()\
                   .setSelectedCols(selectedColNames)
        
        model = trainOp.linkFrom(inOp)
        
        # batch predict
        predictOp = ImputerPredictBatchOp()
        predictOp.linkFrom(model, inOp).print()
        
        # stream predict
        sinOp = StreamOperator.fromDataframe(df_data, schemaStr='col1 string, col2 double, col3 double')
        
        predictStreamOp = ImputerPredictStreamOp(model)
        predictStreamOp.linkFrom(sinOp).print()
        
        StreamOperator.execute()
        pass