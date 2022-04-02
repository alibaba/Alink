import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestFlattenMTableBatchOp(unittest.TestCase):
    def test_flattenmtablebatchop(self):

        import numpy as np
        import pandas as pd
        
        df_data = pd.DataFrame([
              ["a1", "11L", 2.2],
              ["a1", "12L", 2.0],
              ["a2", "11L", 2.0],
              ["a2", "12L", 2.0],
              ["a3", "12L", 2.0],
              ["a3", "13L", 2.0],
              ["a4", "13L", 2.0],
              ["a4", "14L", 2.0],
              ["a5", "14L", 2.0],
              ["a5", "15L", 2.0],
              ["a6", "15L", 2.0],
              ["a6", "16L", 2.0]
        ])
        
        input = BatchOperator.fromDataframe(df_data, schemaStr='id string, f0 string, f1 double')
        
        zip = GroupByBatchOp()\
        	.setGroupByPredicate("id")\
        	.setSelectClause("id, mtable_agg(f0, f1) as m_table_col")
        
        flatten = FlattenMTableBatchOp()\
        	.setReservedCols(["id"])\
        	.setSelectedCol("m_table_col")\
        	.setSchemaStr('f0 string, f1 int')
        
        zip.linkFrom(input).link(flatten).print()
        pass