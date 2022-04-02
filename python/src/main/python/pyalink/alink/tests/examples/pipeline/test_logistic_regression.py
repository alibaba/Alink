import unittest

from pyalink.alink import *


class TestYuhe(unittest.TestCase):

    def test_logistic_regression(self):
        import numpy as np
        import pandas as pd
        data = np.array([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 2]])
        df = pd.DataFrame({"f0": data[:, 0],
                           "f1": data[:, 1],
                           "label": data[:, 2]})

        batchData = dataframeToOperator(df, schemaStr='f0 int, f1 int, label int', op_type='batch')

        colnames = ["f0", "f1"]
        lr = LogisticRegression().setFeatureCols(colnames).setLabelCol("label").setPredictionCol("pred")
        model = lr.fit(batchData)
        model.transform(batchData).print()
