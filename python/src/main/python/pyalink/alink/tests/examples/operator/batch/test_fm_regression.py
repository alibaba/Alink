import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestFmRegression(unittest.TestCase):

    def test_fm_regression(self):
        data = np.array([
            ["1.1 1.0", 1.0],
            ["1.1 1.1", 1.0],
            ["1.1 1.2", 1.0],
            ["0.0 3.2", 0.0],
            ["0.0 4.2", 0.0]
        ])

        df_data = pd.DataFrame({
            "vec": data[:, 0],
            "label": data[:, 1],
        })

        data = dataframeToOperator(df_data, schemaStr='vec string, label double', op_type="batch")

        adagrad = FmRegressorTrainBatchOp()\
            .setVectorCol("vec")\
            .setLabelCol("label")\
            .setNumEpochs(10)\
            .setNumFactor(5)\
            .setInitStdev(0.01)\
            .setLearnRate(0.5)\
            .setEpsilon(0.0001)\
            .linkFrom(data)
        adagrad.lazyPrintModelInfo()
        adagrad.lazyPrintTrainInfo()

        FmRegressorPredictBatchOp()\
            .setVectorCol("vec")\
            .setPredictionCol("pred")\
            .setPredictionDetailCol("details")\
            .linkFrom(adagrad, data)\
            .print()
