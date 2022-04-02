import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestFlattenMTableStreamOp(unittest.TestCase):
    def test_flattenmtablestreamop(self):

        import numpy as np
        import pandas as pd
        df_data = pd.DataFrame([
              ["a1", "{\"data\":{\"f0\":[\"11L\",\"12L\"],\"f1\":[2.0,2.0]},\"schema\":\"f0 VARCHAR,f1 DOUBLE\"}"],
              ["a1", "{\"data\":{\"f0\":[\"13L\",\"14L\"],\"f1\":[2.0,2.0]},\"schema\":\"f0 VARCHAR,f1 DOUBLE\"}"]
        ])
        
        input = StreamOperator.fromDataframe(df_data, schemaStr='id string, mt string')
        
        flatten = FlattenMTableStreamOp()\
        	.setReservedCols(["id"])\
        	.setSelectedCol("mt")\
        	.setSchemaStr('f0 string, f1 int')
        
        input.link(flatten).print()
        StreamOperator.execute()
        pass