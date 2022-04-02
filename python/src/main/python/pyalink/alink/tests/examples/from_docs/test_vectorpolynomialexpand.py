import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorPolynomialExpand(unittest.TestCase):
    def test_vectorpolynomialexpand(self):

        df = pd.DataFrame([
            ["$8$1:3,2:4,4:7"],
            ["$8$2:4,4:5"]
        ])
        data = BatchOperator.fromDataframe(df, schemaStr="vec string")
        VectorPolynomialExpand().setSelectedCol("vec").setOutputCol("vec_out").transform(data).collectToDataframe()
        pass