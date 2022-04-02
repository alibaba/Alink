import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorStandardScaler(unittest.TestCase):
    def test_vectorstandardscaler(self):

        df = pd.DataFrame([
            ["a", "10.0, 100"],
            ["b", "-2.5, 9"],
            ["c", "100.2, 1"],
            ["d", "-99.9, 100"],
            ["a", "1.4, 1"],
            ["b", "-2.2, 9"],
            ["c", "100.9, 1"]
        ])
        data = BatchOperator.fromDataframe(df, schemaStr="col string, vector string")
        model = VectorStandardScaler().setSelectedCol("vector").fit(data)
        model.transform(data).collectToDataframe()
        pass