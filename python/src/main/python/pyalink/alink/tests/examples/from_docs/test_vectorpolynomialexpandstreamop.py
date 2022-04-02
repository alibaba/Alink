import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorPolynomialExpandStreamOp(unittest.TestCase):
    def test_vectorpolynomialexpandstreamop(self):

        df = pd.DataFrame([
            ["$8$1:3,2:4,4:7"],
            ["$8$2:4,4:5"]
        ])
        
        data = StreamOperator.fromDataframe(df, schemaStr="vec string")
        VectorPolynomialExpandStreamOp().setSelectedCol("vec").setOutputCol("vec_out").linkFrom(data).print()
        StreamOperator.execute()
        pass