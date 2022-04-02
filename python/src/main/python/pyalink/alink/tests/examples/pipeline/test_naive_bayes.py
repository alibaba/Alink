import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestYuhe(unittest.TestCase):

    def test_naive_bayes_text_classifier(self):
        data = np.array([
            ["$31$0:1.0 1:1.0 2:1.0 30:1.0", "1.0  1.0  1.0  1.0", '1'],
            ["$31$0:1.0 1:1.0 2:0.0 30:1.0", "1.0  1.0  0.0  1.0", '1'],
            ["$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", '1'],
            ["$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", '1'],
            ["$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", '0'],
            ["$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", '0'],
            ["$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", '0']])

        dataSchema = ["sv", "dv", "label"]

        df = pd.DataFrame({"sv": data[:, 0], "dv": data[:, 1], "label": data[:, 2]})
        batchData = dataframeToOperator(df, schemaStr='sv string, dv string, label string', op_type='batch')

        model = NaiveBayesTextClassifier().setVectorCol("sv").setLabelCol("label").setReservedCols(
            ["sv", "label"]).setPredictionCol("pred")
        model.fit(batchData).transform(batchData).print()
