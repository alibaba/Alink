import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestJsonValueStreamOp(unittest.TestCase):
    def test_jsonvaluestreamop(self):

        df = pd.DataFrame([
             ["{a:boy,b:{b1:[1,2],b2:2}}"],
             ["{a:girl,b:{b1:[1,3],b2:2}}"]
         ])
         
        streamData = StreamOperator.fromDataframe(df, schemaStr='str string')
         
        JsonValueStreamOp()\
             .setJsonPath(["$.a", "$.b.b1[0]","$.b.b2"])\
             .setSelectedCol("str")\
             .setOutputCols(["f0","f1","f2"])\
             .linkFrom(streamData)\
             .print()
             
        StreamOperator.execute()
        pass