import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorSizeHint(unittest.TestCase):
    def test_vectorsizehint(self):

        df = pd.DataFrame([
            ["$8$1:3,2:4,4:7"],
            ["$8$2:4,4:5"]
        ])
        data = BatchOperator.fromDataframe(df, schemaStr="vec string")
        model = VectorSizeHint().setSelectedCol("vec").setOutputCol("vec_hint").setHandleInvalidMethod("SKIP").setSize(8)
        model.transform(data).collectToDataframe()
        pass