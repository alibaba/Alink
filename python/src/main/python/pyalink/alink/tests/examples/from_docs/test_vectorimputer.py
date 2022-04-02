import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorImputer(unittest.TestCase):
    def test_vectorimputer(self):

        df = pd.DataFrame([
            ["1:3,2:4,4:7", 1],
            ["1:3,2:NaN", 3],
            ["2:4,4:5", 4]])
        data = BatchOperator.fromDataframe(df, schemaStr="vec string, id bigint")
        vecFill = VectorImputer().setSelectedCol("vec").setOutputCol("vec1")
        model = vecFill.fit(data)
        model.transform(data).collectToDataframe()
        pass