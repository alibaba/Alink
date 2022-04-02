import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestDeepARPredictBatchOp(unittest.TestCase):
    def test_deeparpredictbatchop(self):

        import time, datetime
        import numpy as np
        import pandas as pd
        
        data = pd.DataFrame([
            [0,  datetime.datetime.fromisoformat('2021-11-01 00:00:00'), 100.0],
            [0,  datetime.datetime.fromisoformat('2021-11-02 00:00:00'), 100.0],
            [0,  datetime.datetime.fromisoformat('2021-11-03 00:00:00'), 100.0],
            [0,  datetime.datetime.fromisoformat('2021-11-04 00:00:00'), 100.0],
            [0,  datetime.datetime.fromisoformat('2021-11-05 00:00:00'), 100.0]
        ])
        
        source = dataframeToOperator(data, schemaStr='id int, ts timestamp, series double', op_type='batch')
        
        deepARTrainBatchOp = DeepARTrainBatchOp()\
            .setTimeCol("ts")\
            .setSelectedCol("series")\
            .setNumEpochs(10)\
            .setWindow(2)\
            .setStride(1)
        
        groupByBatchOp = GroupByBatchOp()\
            .setGroupByPredicate("id")\
            .setSelectClause("mtable_agg(ts, series) as mtable_agg_series")
        
        deepARPredictBatchOp = DeepARPredictBatchOp()\
                    .setPredictNum(2)\
                    .setPredictionCol("pred")\
                    .setValueCol("mtable_agg_series")
        
        deepARPredictBatchOp\
            .linkFrom(
                deepARTrainBatchOp.linkFrom(source),
                groupByBatchOp.linkFrom(source)
            )\
            .print()
        pass