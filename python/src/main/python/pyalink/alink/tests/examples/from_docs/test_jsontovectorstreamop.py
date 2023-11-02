import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestJsonToVectorStreamOp(unittest.TestCase):
    def test_jsontovectorstreamop(self):

        df = pd.DataFrame([
            ['1', '{"0":"1.0","1":"2.0"}', '$3$0:1.0 1:2.0', '1:1.0,2:2.0', '1.0,2.0', 1.0, 2.0],
            ['2', '{"0":"4.0","1":"8.0"}', '$3$0:4.0 1:8.0', '1:4.0,2:8.0', '4.0,8.0', 4.0, 8.0]])
        
        data = StreamOperator.fromDataframe(df, schemaStr="row string, json string, vec string, kv string, csv string, f0 double, f1 double")
            
        op = JsonToVectorStreamOp()\
            .setJsonCol("json")\
            .setReservedCols(["row"])\
            .setVectorCol("vec")\
            .setVectorSize(5)\
            .linkFrom(data)
        
        op.print()
        
        StreamOperator.execute()
        pass