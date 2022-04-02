import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestFlattenKObjectStreamOp(unittest.TestCase):
    def test_flattenkobjectstreamop(self):

        df_data = pd.DataFrame([
            [1,'{"rating":"[0.6]","object":"[1]"}'],
            [2,'{"rating":"[0.8,0.6]","object":"[2,3]"}'],
            [3,'{"rating":"[0.6,0.3,0.4]","object":"[1,2,3]"}']
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr='user bigint, rec string')
        sdata = StreamOperator.fromDataframe(df_data, schemaStr='user bigint, rec string')
        
        recList = FlattenKObjectStreamOp()\
        			.setSelectedCol("rec")\
        			.setOutputCols(["object", "rating"])\
        			.setOutputColTypes(["long", "double"])\
        			.setReservedCols(["user"])\
        			.linkFrom(sdata).print();
        StreamOperator.execute()
        pass