import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestToTensor(unittest.TestCase):
    def test_totensor(self):

        df = pd.DataFrame(["FLOAT#6#0.0 0.1 1.0 1.1 2.0 2.1 "])
        source = BatchOperator.fromDataframe(df, schemaStr='vec string')
        
        toTensor = ToTensor() \
            .setSelectedCol("vec") \
            .setTensorShape([2, 3]) \
            .setTensorDataType("float")
        toTensor.transform(source).print()
        pass