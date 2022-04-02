import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestEvalTimeSeriesBatchOp(unittest.TestCase):
    def test_evaltimeseriesbatchop(self):

        import time, datetime
        import numpy as np
        import pandas as pd
        
        data = pd.DataFrame([
        			[1, datetime.datetime.fromtimestamp(1), 10.0, 10.5],
        			[1, datetime.datetime.fromtimestamp(2), 11.0, 10.5],
        			[1, datetime.datetime.fromtimestamp(3), 12.0, 11.5],
        			[1, datetime.datetime.fromtimestamp(4), 13.0, 12.5],
        			[1, datetime.datetime.fromtimestamp(5), 14.0, 13.5],
        			[1, datetime.datetime.fromtimestamp(6), 15.0, 14.5],
        			[1, datetime.datetime.fromtimestamp(7), 16.0, 14.5],
        			[1, datetime.datetime.fromtimestamp(8), 17.0, 14.5],
        			[1, datetime.datetime.fromtimestamp(9), 18.0, 14.5],
        			[1, datetime.datetime.fromtimestamp(10), 19.0, 16.5]
        ])
        
        source = dataframeToOperator(data, schemaStr='id int, ts timestamp, val double, pred double', op_type='batch')
        
        cmex = source.link(
            EvalTimeSeriesBatchOp()\
                .setLabelCol("val")\
                .setPredictionCol("pred")
        ).collectMetrics()
        
        print(cmex.getMse())
        print(cmex.getMae())
        print(cmex.getRmse())
        print(cmex.getSse())
        print(cmex.getSst())
        print(cmex.getSsr())
        print(cmex.getSae())
        print(cmex.getMape())
        print(cmex.getSmape())
        print(cmex.getND())
        print(cmex.getCount())
        print(cmex.getYMean())
        print(cmex.getPredictionMean())
        pass