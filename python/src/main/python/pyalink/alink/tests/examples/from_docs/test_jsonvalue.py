import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestJsonValue(unittest.TestCase):
    def test_jsonvalue(self):

        df = pd.DataFrame([
            ["{a:boy,b:{b1:1,b2:2}}"],
            ["{a:girl,b:{b1:1,b2:2}}"]])
        
        batchData = BatchOperator.fromDataframe(df, schemaStr='str string')
        
        JsonValue()\
            .setJsonPath(["$.a","$.b.b1"])\
            .setSelectedCol("str")\
            .setOutputCols(["f0","f1"])\
            .transform(batchData)\
            .print()
        pass