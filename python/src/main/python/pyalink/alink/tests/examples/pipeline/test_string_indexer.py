import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestSpecialModels(unittest.TestCase):

    def test_run(self):
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

        data = dataframeToOperator(df_data, schemaStr='f0 string', op_type='batch')

        stringIndexer = StringIndexer() \
            .setModelName("string_indexer_model") \
            .setSelectedCol("f0") \
            .setOutputCol("f0_indexed") \
            .setStringOrderType("frequency_asc")

        model = stringIndexer.fit(data)
        indexed = model.transform(data)
        indexed.print()
