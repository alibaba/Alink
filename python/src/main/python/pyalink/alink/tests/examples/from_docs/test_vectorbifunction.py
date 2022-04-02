import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorBiFunction(unittest.TestCase):
    def test_vectorbifunction(self):

        df = pd.DataFrame([
            ["1 2 3", "2 3 4"]
        ])
        data = BatchOperator.fromDataframe(df, schemaStr="vec1 string, vec2 string")
        VectorBiFunction() \
        		.setSelectedCols(["vec1", "vec2"]) \
        		.setBiFuncName("minus").setOutputCol("vec_minus").transform(data).print();
        pass