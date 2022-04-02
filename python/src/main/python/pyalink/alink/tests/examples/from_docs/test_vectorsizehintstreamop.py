import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorSizeHintStreamOp(unittest.TestCase):
    def test_vectorsizehintstreamop(self):

        df = pd.DataFrame([
            ["$8$1:3,2:4,4:7"],
            ["$8$2:4,4:5"]
        ])
        data = StreamOperator.fromDataframe(df, schemaStr="vec string")
        VectorSizeHintStreamOp().setSelectedCol("vec").setOutputCol("vec_hint").setHandleInvalidMethod("Skip").setSize(3).linkFrom(data).print()
        StreamOperator.execute()
        pass