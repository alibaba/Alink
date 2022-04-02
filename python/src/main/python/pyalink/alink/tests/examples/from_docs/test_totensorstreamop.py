import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestToTensorStreamOp(unittest.TestCase):
    def test_totensorstreamop(self):

        df = pd.DataFrame(["FLOAT#6#0.0 0.1 1.0 1.1 2.0 2.1 "])
        source = StreamOperator.fromDataframe(df, schemaStr='vec string')
        
        source.link(
            ToTensorStreamOp()
                .setSelectedCol("vec")
                .setTensorShape([2, 3])
                .setTensorDataType("float")
        ).print()
        StreamOperator.execute()
        pass