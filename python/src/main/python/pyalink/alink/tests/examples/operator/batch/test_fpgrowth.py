import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestFpGrowth(unittest.TestCase):

    def test_fpgrowth(self):
        data = np.array([
            ["A,B,C,D"],
            ["B,C,E"],
            ["A,B,C,E"],
            ["B,D,E"],
            ["A,B,C,D"],
        ])

        df_data = pd.DataFrame({
            "items": data[:, 0],
        })

        data = dataframeToOperator(df_data, schemaStr='items string', op_type="batch")

        fpGrowth = FpGrowthBatchOp() \
            .setItemsCol("items") \
            .setMinSupportPercent(0.4) \
            .setMinConfidence(0.6)

        fpGrowth.linkFrom(data)

        fpGrowth.print()
        fpGrowth.getSideOutput(0).print()
