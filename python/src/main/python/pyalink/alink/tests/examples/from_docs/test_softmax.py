import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestSoftmax(unittest.TestCase):
    def test_softmax(self):

        df_data = pd.DataFrame([
               [2, 1, 1],
               [3, 2, 1],
               [4, 3, 2],
               [2, 4, 1],
               [2, 2, 1],
               [4, 3, 2],
               [1, 2, 1],
               [5, 3, 3]
        ])
        
        batchData = BatchOperator.fromDataframe(df_data, schemaStr='f0 int, f1 int, label int')
        dataTest = batchData
        colnames = ["f0","f1"]
        softmax = Softmax().setFeatureCols(colnames).setLabelCol("label").setPredictionCol("pred")
        model = softmax.fit(batchData)
        
        model.transform(dataTest).print()
        pass