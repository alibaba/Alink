import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestProphetPredictStreamOp(unittest.TestCase):
    def test_prophetpredictstreamop(self):

        import time, datetime
        import numpy as np
        import pandas as pd
        
        data = pd.DataFrame([
        			[1,  datetime.datetime.fromtimestamp(1000), 10.0],
        			[1,  datetime.datetime.fromtimestamp(2000), 11.0],
        			[1,  datetime.datetime.fromtimestamp(3000), 12.0],
        			[1,  datetime.datetime.fromtimestamp(4000), 13.0],
        			[1,  datetime.datetime.fromtimestamp(5000), 14.0],
        			[1,  datetime.datetime.fromtimestamp(6000), 15.0],
        			[1,  datetime.datetime.fromtimestamp(7000), 16.0],
        			[1,  datetime.datetime.fromtimestamp(8000), 17.0],
        			[1,  datetime.datetime.fromtimestamp(9000), 18.0],
        			[1,  datetime.datetime.fromtimestamp(10000), 19.0]
        ])
        
        streamSource = dataframeToOperator(data, schemaStr='id int, ds1 timestamp, y1 double', op_type='stream')
        
        over = OverCountWindowStreamOp()\
        			.setTimeCol("ds1")\
        			.setPrecedingRows(4)\
        			.setClause("mtable_agg_preceding(ds1,y1) as tensor")
        
        streamPred = ProphetStreamOp()\
        			.setValueCol("tensor")\
        			.setPredictNum(1)\
        			.setPredictionCol("pred")\
        			.setPredictionDetailCol("pred_detail")
        
        valueOp = LookupVectorInTimeSeriesStreamOp()\
        			.setTimeSeriesCol("pred")\
        			.setTimeCol("ds1")\
        			.setReservedCols(["ds1", "tensor", "pred"])\
        			.setOutputCol("y_hat")
        
        streamSource\
            .link(over)\
            .link(streamPred)\
            .link(valueOp)\
            .print()
        
        StreamOperator.execute()
        pass