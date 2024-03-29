import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLookupVectorInTimeSeriesBatchOp(unittest.TestCase):
    def test_lookupvectorintimeseriesbatchop(self):

        import time, datetime
        import numpy as np
        import pandas as pd
        
        data = pd.DataFrame([
        			[1,  datetime.datetime.fromtimestamp(1), "10.0 10.0"],
        			[1,  datetime.datetime.fromtimestamp(2), "11.0 11.0"],
        			[1,  datetime.datetime.fromtimestamp(3), "12.0 12.0"],
        			[1,  datetime.datetime.fromtimestamp(4), "13.0 13.0"],
        			[1,  datetime.datetime.fromtimestamp(5), "14.0 14.0"],
        			[1,  datetime.datetime.fromtimestamp(6), "15.0 15.0"],
        			[1,  datetime.datetime.fromtimestamp(7), "16.0 16.0"],
        			[1,  datetime.datetime.fromtimestamp(8), "17.0 17.0"],
        			[1,  datetime.datetime.fromtimestamp(9), "18.0 18.0"],
        			[1,  datetime.datetime.fromtimestamp(10), "19.0 19.0"]
        ])
        
        source = dataframeToOperator(data, schemaStr='id int, ts timestamp, val string', op_type='batch')
        
        source.link(
                GroupByBatchOp()
        			.setGroupByPredicate("id")
        			.setSelectClause("id, mtable_agg(ts, val) as data")
        		).link(
                    ShiftBatchOp()
        					.setValueCol("data")
        					.setShiftNum(7)
        					.setPredictNum(12)
        					.setPredictionCol("predict")
        		).link(
                    FlattenMTableBatchOp()
        					.setReservedCols(["id", "predict"])
        					.setSelectedCol("predict")
        					.setSchemaStr("ts timestamp, val VECTOR")
                ).link(
                    LookupVectorInTimeSeriesBatchOp()
        				.setTimeCol("ts")
        				.setTimeSeriesCol("predict")
        				.setOutputCol("out")
        				.setReservedCols(["id"])
                ).print()
        pass