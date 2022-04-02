import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorInteractionStreamOp(unittest.TestCase):
    def test_vectorinteractionstreamop(self):

        df = pd.DataFrame([
            ["$8$1:3,2:4,4:7", "$8$1:3,2:4,4:7"],
            ["$8$0:3,5:5", "$8$1:2,2:4,4:7"],
            ["$8$2:4,4:5", "$8$1:3,2:3,4:7"]
        ])
        
        data = StreamOperator.fromDataframe(df, schemaStr="vec1 string, vec2 string")
        vecInter = VectorInteractionStreamOp().setSelectedCols(["vec1","vec2"]).setOutputCol("vec_product")
        data.link(vecInter).print()
        StreamOperator.execute()
        pass