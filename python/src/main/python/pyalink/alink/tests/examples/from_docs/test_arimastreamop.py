import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestArimaStreamOp(unittest.TestCase):
    def test_arimastreamop(self):

        import time, datetime
        import numpy as np
        import pandas as pd
        
        data = pd.DataFrame([
        			[1,  datetime.datetime.fromtimestamp(1), 10.0],
        			[1,  datetime.datetime.fromtimestamp(2), 11.0],
        			[1,  datetime.datetime.fromtimestamp(3), 12.0],
        			[1,  datetime.datetime.fromtimestamp(4), 13.0],
        			[1,  datetime.datetime.fromtimestamp(5), 14.0],
        			[1,  datetime.datetime.fromtimestamp(6), 15.0],
        			[1,  datetime.datetime.fromtimestamp(7), 16.0],
        			[1,  datetime.datetime.fromtimestamp(8), 17.0],
        			[1,  datetime.datetime.fromtimestamp(9), 18.0],
        			[1,  datetime.datetime.fromtimestamp(10), 19.0]
        ])
        
        source = dataframeToOperator(data, schemaStr='id int, ts timestamp, val double', op_type='stream')
        
        source.link(
        			OverCountWindowStreamOp()
        				.setGroupCols(["id"])
        				.setTimeCol("ts")
        				.setPrecedingRows(5)
        				.setClause("mtable_agg_preceding(ts, val) as data")
        		).link(
        			ArimaStreamOp()
        				.setValueCol("data")
        				.setOrder([1, 2, 1])
        				.setPredictNum(12)
        				.setPredictionCol("predict")
        		).link(
        			LookupValueInTimeSeriesStreamOp()
        				.setTimeCol("ts")
        				.setTimeSeriesCol("predict")
        				.setOutputCol("out")
        		).print()
        
        StreamOperator.execute()
        pass