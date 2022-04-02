import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def test_chi_sq_selector_op(self):
        data = np.array([
            ["a", 1, 1, 2.0, True],
            [None, 2, 2, -3.0, True],
            ["c", None, None, 2.0, False],
            ["a", 0, 0, None, None]
        ])
        df = pd.DataFrame({"f_string": data[:, 0], "f_long": data[:, 1], "f_int": data[:, 2], "f_double": data[:, 3],
                           "f_boolean": data[:, 4]})

        source = BatchOperator\
            .fromDataframe(df, schemaStr='f_string string, f_long long, f_int int, f_double double, f_boolean boolean')

        selector = ChiSqSelectorBatchOp() \
            .setSelectedCols(["f_string", "f_long", "f_int", "f_double"]) \
            .setLabelCol("f_boolean") \
            .setNumTopFeatures(2)

        selector.linkFrom(source)

        from pyalink.alink.common.types.model_info import ChisqSelectorModelInfo

        def model_info_callback(d: ChisqSelectorModelInfo):
            self.assertEquals(type(d), ChisqSelectorModelInfo)
            print(d.getFdr())

        selector.lazyCollectModelInfo(model_info_callback)

        model_info: ChisqSelectorModelInfo = selector.collectModelInfo()
        print(model_info)
