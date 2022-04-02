import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def run_sample_op(self):
        data = data = np.array([
            ["0,0,0"],
            ["0.1,0.1,0.1"],
            ["0.2,0.2,0.2"],
            ["9,9,9"],
            ["9.1,9.1,9.1"],
            ["9.2,9.2,9.2"]
        ])

        df = pd.DataFrame({"Y": data[:, 0]})

        # batch source
        inOp = dataframeToOperator(df, schemaStr='Y string', op_type='batch')

        sampleOp = SampleBatchOp() \
            .setRatio(0.3) \
            .setWithReplacement(False)

        inOp.link(sampleOp).print()
