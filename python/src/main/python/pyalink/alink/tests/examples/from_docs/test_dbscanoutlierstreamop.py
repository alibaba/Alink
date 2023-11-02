import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestDbscanOutlierStreamOp(unittest.TestCase):
    def test_dbscanoutlierstreamop(self):

        import time, datetime
        import numpy as np
        import pandas as pd
        
        data = pd.DataFrame([
        			[0,1, 49.0, 2.0, datetime.datetime.fromtimestamp(1512057600000 / 1000)],
        			[1,1, 50.0, 2.0, datetime.datetime.fromtimestamp(1512144000000 / 1000)],
        			[2,1, 50.0, 1.0, datetime.datetime.fromtimestamp(1512230400000 / 1000)],
        			[3,0, 2.0, 5.0, datetime.datetime.fromtimestamp(1512316800000 / 1000)],
        			[4,0, 2.0, 5.0, datetime.datetime.fromtimestamp(1512403200000 / 1000)],
        			[5,0, 3.0, 4.0, datetime.datetime.fromtimestamp(1512489600000 / 1000)],
        			[6,0, 3.0, 4.0, datetime.datetime.fromtimestamp(1512576000000 / 1000)],
        			[7,0, 3.0, 4.0, datetime.datetime.fromtimestamp(1512662400000 / 1000)],
        			[8,0, 3.0, 4.0, datetime.datetime.fromtimestamp(1512748800000 / 1000)],
        			[9,0, 3.0, 4.0, datetime.datetime.fromtimestamp(1512835200000 / 1000)]
        ])
        dataOp = dataframeToOperator(data, schemaStr='id int, group_id int, f0 double, f1 double, ts timestamp', op_type='stream')
        outlierOp = DbscanOutlierStreamOp()\
        			.setGroupCols(["group_id"])\
        			.setTimeCol("ts")\
        			.setMinPoints(3)\
        			.setFeatureCols(["f0"])\
        			.setPredictionCol("pred")\
        			.setPredictionDetailCol("pred_detail")
        
        dataOp.link(outlierOp).print()
        
        StreamOperator.execute()
        pass