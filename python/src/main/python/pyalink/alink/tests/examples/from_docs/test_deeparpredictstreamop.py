import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestDeepARPredictStreamOp(unittest.TestCase):
    def test_deeparpredictstreamop(self):

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
        
        batch_source = dataframeToOperator(data, schemaStr='id int, ts timestamp, series double', op_type='batch')
        stream_source = dataframeToOperator(data, schemaStr='id int, ts timestamp, series double', op_type='stream')
        
        deepARTrainBatchOp = DeepARTrainBatchOp()\
            .setTimeCol("ts")\
            .setSelectedCol("series")\
            .setNumEpochs(10)\
            .setWindow(2)\
            .setStride(1)\
            .linkFrom(batch_source)
        
        overCountWindowStreamOp = OverCountWindowStreamOp()\
            .setClause("MTABLE_AGG_PRECEDING(ts, series) as mtable_agg_series")\
            .setTimeCol("ts")\
            .setPrecedingRows(2)
        
        deepARPredictStreamOp = DeepARPredictStreamOp(deepARTrainBatchOp)\
            .setPredictNum(2)\
            .setPredictionCol("pred")\
            .setValueCol("mtable_agg_series")
        
        deepARPredictStreamOp\
            .linkFrom(
                overCountWindowStreamOp\
                .linkFrom(stream_source)\
                .filter("ts = TO_TIMESTAMP('2021-11-05 00:00:00')")
            )\
            .print()
        
        StreamOperator.execute()
        pass