import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestFlattenKObjectBatchOp(unittest.TestCase):
    def test_flattenkobjectbatchop(self):

        df_data = pd.DataFrame([
            [1, 1, 0.6],
            [2, 2, 0.8],
            [2, 3, 0.6],
            [4, 1, 0.6],
            [4, 2, 0.3],
            [4, 3, 0.4],
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr='user bigint, item bigint, rating double')
        
        jsonData = Zipped2KObjectBatchOp()\
        			.setGroupCol("user")\
                    .setObjectCol("item")\
        			.setInfoCols(["rating"])\
        			.setOutputCol("recomm")\
        			.linkFrom(data)\
        			.lazyPrint(-1);
        recList = FlattenKObjectBatchOp()\
        			.setSelectedCol("recomm")\
        			.setOutputCols(["item", "rating"])\
            		.setOutputColTypes(["long", "double"])\
        			.setReservedCols(["user"])\
        			.linkFrom(jsonData)\
        			.lazyPrint(-1);
        BatchOperator.execute();
        pass