import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestProphetBatchOp(unittest.TestCase):
    def test_prophetbatchop(self):

        import time, datetime
        import numpy as np
        import pandas as pd
        
        downloader = AlinkGlobalConfiguration.getPluginDownloader()
        downloader.downloadPlugin('tf115_python_env_linux')
        
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
        
        source = dataframeToOperator(data, schemaStr='id int, ts timestamp, val double', op_type='batch')
        
        source.link(
                GroupByBatchOp()
        			.setGroupByPredicate("id")
        			.setSelectClause("id, mtable_agg(ts, val) as data")
        		).link(ProphetBatchOp()
        			.setValueCol("data")
        			.setPredictNum(4)
        			.setPredictionCol("pred")
        		).print()
        pass