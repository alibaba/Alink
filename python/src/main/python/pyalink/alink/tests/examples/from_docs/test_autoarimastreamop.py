import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestAutoArimaStreamOp(unittest.TestCase):
    def test_autoarimastreamop(self):

        import time, datetime
        import numpy as np
        import pandas as pd
        
        data = pd.DataFrame([
        			[1,  datetime.datetime.fromtimestamp(1001), 10.0],
        			[1,  datetime.datetime.fromtimestamp(1002), 11.0],
        			[1,  datetime.datetime.fromtimestamp(1003), 12.0],
        			[1,  datetime.datetime.fromtimestamp(1004), 13.0],
        			[1,  datetime.datetime.fromtimestamp(1005), 14.0],
        			[1,  datetime.datetime.fromtimestamp(1006), 15.0],
        			[1,  datetime.datetime.fromtimestamp(1007), 16.0],
        			[1,  datetime.datetime.fromtimestamp(1008), 17.0],
        			[1,  datetime.datetime.fromtimestamp(1009), 18.0],
        			[1,  datetime.datetime.fromtimestamp(1010), 19.0]
        ])
        
        source = dataframeToOperator(data, schemaStr='id int, ts timestamp, val double', op_type='stream')
        
        source.link(
        			OverCountWindowStreamOp()
        				.setGroupCols(["id"])
        				.setTimeCol("ts")
        				.setPrecedingRows(5)
        				.setClause("mtable_agg_preceding(ts, val) as data")
        		).link(
        			AutoArimaStreamOp()
        				.setValueCol("data")
        				.setPredictionCol("predict")
                        .setMaxOrder(1)
        				.setPredictNum(4)
        		).link(
        			LookupValueInTimeSeriesStreamOp()
        				.setTimeCol("ts")
        				.setTimeSeriesCol("predict")
        				.setOutputCol("out")
        		).print()
        
        StreamOperator.execute()
        pass