import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLogisticRegression(unittest.TestCase):
    def test_logisticregression(self):

        df_data = pd.DataFrame([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 2]
        ])
        
        batchData = BatchOperator.fromDataframe(df_data, schemaStr='f0 int, f1 int, label int')
        
        colnames = ["f0","f1"]
        lr = LogisticRegression().setFeatureCols(colnames).setLabelCol("label").setPredictionCol("pred")
        model = lr.fit(batchData)
        model.transform(batchData).print()
        pass