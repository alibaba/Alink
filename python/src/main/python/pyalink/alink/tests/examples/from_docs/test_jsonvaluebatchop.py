import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestJsonValueBatchOp(unittest.TestCase):
    def test_jsonvaluebatchop(self):

        df = pd.DataFrame([
             ["{a:boy,b:{b1:[1,2],b2:2}}"],
             ["{a:girl,b:{b1:[1,3],b2:2}}"]
         ])
         
        data = BatchOperator.fromDataframe(df, schemaStr='str string')
         
        JsonValueBatchOp()\
             .setJsonPath(["$.a", "$.b.b1[0]","$.b.b2"])\
             .setSelectedCol("str")\
             .setOutputCols(["f0","f1","f2"])\
             .linkFrom(data)\
             .print()
        pass