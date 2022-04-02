import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestKSigmaOutlierStreamOp(unittest.TestCase):
    def test_ksigmaoutlierstreamop(self):

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
        
        outlierOp = KSigmaOutlierStreamOp()\
        			.setGroupCols(["id"])\
        			.setTimeCol("ts")\
        			.setPrecedingRows(3)\
        			.setFeatureCol("val")\
        			.setPredictionCol("pred")\
        			.setPredictionDetailCol("pred_detail")
        
        dataOp.link(outlierOp).print()
        
        StreamOperator.execute()
        pass