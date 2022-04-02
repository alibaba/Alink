import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestQingzhao(unittest.TestCase):

    def test_vector_size_hint(self):
        data = np.array([["$8$1:3,2:4,4:7"], ["$8$2:4,4:5"]])
        df = pd.DataFrame({"vec": data[:, 0]})
        data = dataframeToOperator(df, schemaStr="vec string", op_type="batch")
        model = VectorSizeHint().setSelectedCol("vec").setOutputCol("vec_hint").setHandleInvalidMethod(
            "SKIP").setSize(8)
        model.transform(data).collectToDataframe()
