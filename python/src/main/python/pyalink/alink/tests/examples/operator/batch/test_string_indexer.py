import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestStringIndexer(unittest.TestCase):

    def test_predict(self):
        data = np.array([
            ["football"],
            ["football"],
            ["football"],
            ["basketball"],
            ["basketball"],
            ["tennis"],
        ])

        df_data = pd.DataFrame({
            "f0": data[:, 0],
        })

        data = dataframeToOperator(df_data, schemaStr='f0 string', op_type="batch")

        stringindexer = StringIndexerTrainBatchOp() \
            .setSelectedCol("f0") \
            .setStringOrderType("frequency_asc")

        predictor = StringIndexerPredictBatchOp().setSelectedCol("f0").setOutputCol("f0_indexed")

        model = stringindexer.linkFrom(data)
        predictor.linkFrom(model, data).print()

    def test_train(self):
        data = np.array([
            ["football"],
            ["football"],
            ["football"],
            ["basketball"],
            ["basketball"],
            ["tennis"],
        ])

        df_data = pd.DataFrame({
            "f0": data[:, 0],
        })

        data = dataframeToOperator(df_data, schemaStr='f0 string', op_type="batch")

        stringindexer = StringIndexerTrainBatchOp() \
            .setSelectedCol("f0") \
            .setStringOrderType("frequency_asc")

        model = stringindexer.linkFrom(data)
        model.print()
