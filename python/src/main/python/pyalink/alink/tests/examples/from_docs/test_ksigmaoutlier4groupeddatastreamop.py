import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestKSigmaOutlier4GroupedDataStreamOp(unittest.TestCase):
    def test_ksigmaoutlier4groupeddatastreamop(self):

        import time, datetime
        import numpy as np
        import pandas as pd
        
        data = pd.DataFrame([
        			[1, datetime.datetime.fromtimestamp(1), 10.0, 0],
        			[1, datetime.datetime.fromtimestamp(2), 11.0, 0],
        			[1, datetime.datetime.fromtimestamp(3), 12.0, 0],
        			[1, datetime.datetime.fromtimestamp(4), 13.0, 0],
        			[1, datetime.datetime.fromtimestamp(5), 14.0, 0],
        			[1, datetime.datetime.fromtimestamp(6), 15.0, 0],
        			[1, datetime.datetime.fromtimestamp(7), 16.0, 0],
        			[1, datetime.datetime.fromtimestamp(8), 17.0, 0],
        			[1, datetime.datetime.fromtimestamp(9), 18.0, 0],
        			[1, datetime.datetime.fromtimestamp(10), 19.0, 0]
        ])
        
        dataOp = dataframeToOperator(data, schemaStr='id int, ts timestamp, val double, label int', op_type='stream')
        
        dataOp.link(\
        			OverCountWindowStreamOp()\
        				.setGroupCols(["id"])\
        				.setTimeCol("ts")\
        				.setPrecedingRows(5)\
        				.setClause("MTABLE_AGG_PRECEDING(ts, val) as series_data")\
        				.setReservedCols(["id", "label"])\
        		).link(\
        			KSigmaOutlier4GroupedDataStreamOp()\
        				.setInputMTableCol("series_data")\
        				.setFeatureCol("val")\
        				.setOutputMTableCol("output_series")\
        				.setPredictionCol("pred")\
        		).link(\
        			FlattenMTableStreamOp()\
        				.setSelectedCol("output_series")\
        				.setSchemaStr("ts TIMESTAMP, val DOUBLE, pred BOOLEAN")\
        		).print()
        
        
        StreamOperator.execute()
        pass