import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestQingzhao(unittest.TestCase):

    def run_vector_standard_scaler(self):
        data = np.array([["a", "10.0, 100"], \
                         ["b", "-2.5, 9"], \
                         ["c", "100.2, 1"], \
                         ["d", "-99.9, 100"], \
                         ["a", "1.4, 1"], \
                         ["b", "-2.2, 9"], \
                         ["c", "100.9, 1"]])
        df = pd.DataFrame({"col": data[:, 0], "vector": data[:, 1]})
        data = dataframeToOperator(df, schemaStr="col string, vector string", op_type="batch")
        VectorStandardScaler().setSelectedCol("vector").fit(data).transform(data).collectToDataframe()
