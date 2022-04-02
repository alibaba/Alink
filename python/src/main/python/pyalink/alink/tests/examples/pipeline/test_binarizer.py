import unittest

from pyalink.alink import *


class TestPinjiu(unittest.TestCase):

    def test_binarizer(self):
        import numpy as np
        import pandas as pd
        data = np.array([
            [1.1, True, "2", "A"],
            [1.1, False, "2", "B"],
            [1.1, True, "1", "B"],
            [2.2, True, "1", "A"]
        ])
        df = pd.DataFrame({"double": data[:, 0], "bool": data[:, 1], "number": data[:, 2], "str": data[:, 3]})

        inOp = BatchOperator.fromDataframe(df, schemaStr='double double, bool boolean, number int, str string')
        binarizer = Binarizer().setSelectedCol("double").setThreshold(2.0)
        binarizer.transform(inOp).print()
