import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestKvToColumns(unittest.TestCase):
    def test_kvtocolumns(self):

        df = pd.DataFrame([
            ['1', '{"f0":"1.0","f1":"2.0"}', '$3$0:1.0 1:2.0', 'f0:1.0,f1:2.0', '1.0,2.0', 1.0, 2.0],
            ['2', '{"f0":"4.0","f1":"8.0"}', '$3$0:4.0 1:8.0', 'f0:4.0,f1:8.0', '4.0,8.0', 4.0, 8.0]])
        
        data = BatchOperator.fromDataframe(df, schemaStr="row string, json string, vec string, kv string, csv string, f0 double, f1 double")
         
        op = KvToColumns()\
            .setKvCol("kv")\
            .setReservedCols(["row"])\
            .setSchemaStr("f0 double, f1 double")\
            .transform(data)
        
        op.print()
        pass