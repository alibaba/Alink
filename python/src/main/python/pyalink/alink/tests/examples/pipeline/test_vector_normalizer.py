import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestQingzhao(unittest.TestCase):

    def test_vector_normalizer(self):
        data = np.array([["1:3,2:4,4:7", 1], \
                         ["0:3,5:5", 3], \
                         ["2:4,4:5", 4]])
        df = pd.DataFrame({"vec": data[:, 0], "id": data[:, 1]})
        data = dataframeToOperator(df, schemaStr="vec string, id bigint", op_type="batch")
        VectorNormalizer().setSelectedCol("vec").setOutputCol("vec_norm").transform(data).print()
