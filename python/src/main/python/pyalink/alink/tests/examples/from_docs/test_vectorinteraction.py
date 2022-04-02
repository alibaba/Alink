import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorInteraction(unittest.TestCase):
    def test_vectorinteraction(self):

        df = pd.DataFrame([
            ["$8$1:3,2:4,4:7", "$8$1:3,2:4,4:7"],
            ["$8$0:3,5:5", "$8$1:2,2:4,4:7"],
            ["$8$2:4,4:5", "$8$1:3,2:3,4:7"]
        ])
        data = BatchOperator.fromDataframe(df, schemaStr="vec1 string, vec2 string")
        vecInter = VectorInteraction().setSelectedCols(["vec1","vec2"]).setOutputCol("vec_product")
        vecInter.transform(data).collectToDataframe()
        pass