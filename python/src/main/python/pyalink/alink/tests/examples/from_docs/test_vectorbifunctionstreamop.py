import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorBiFunctionStreamOp(unittest.TestCase):
    def test_vectorbifunctionstreamop(self):

        df = pd.DataFrame([
            ["1 2 3", "2 3 4"]
        ])
        data = StreamOperator.fromDataframe(df, schemaStr="vec1 string, vec2 string")
        VectorBiFunctionStreamOp() \
        		.setSelectedCols(["vec1", "vec2"]) \
        		.setBiFuncName("minus").setOutputCol("vec_minus").linkFrom(data).print();
        StreamOperator.execute()
        pass