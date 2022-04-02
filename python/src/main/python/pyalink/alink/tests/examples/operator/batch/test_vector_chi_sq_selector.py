import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def test_vector_chi_sq_selector_op(self):
        data = np.array([
            ["1.0 2.0 4.0", "a"],
            ["-1.0 -3.0 4.0", "a"],
            ["4.0 2.0 3.0", "b"],
            ["3.4 5.1 5.0", "b"]
        ])
        df = pd.DataFrame.from_records(data, columns=["vec", "label"])

        source = BatchOperator.fromDataframe(df, schemaStr="vec string, label string")
        source.print()

        selector = VectorChiSqSelectorBatchOp() \
            .setSelectedCol("vec") \
            .setLabelCol("label") \
            .setNumTopFeatures(2)
        selector.linkFrom(source)

        selectedIndices = selector.collectModelInfo()

        print(selectedIndices)
