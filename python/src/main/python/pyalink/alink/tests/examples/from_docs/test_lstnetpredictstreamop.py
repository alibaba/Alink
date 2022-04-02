import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestLSTNetPredictStreamOp(unittest.TestCase):
    def test_lstnetpredictstreamop(self):

        import time, datetime
        import numpy as np
        import pandas as pd
        
        data = pd.DataFrame([
            [0, datetime.datetime.fromisoformat("2021-11-01 00:00:00"), 100.0],
            [0, datetime.datetime.fromisoformat("2021-11-02 00:00:00"), 200.0],
            [0, datetime.datetime.fromisoformat("2021-11-03 00:00:00"), 300.0],
            [0, datetime.datetime.fromisoformat("2021-11-04 00:00:00"), 400.0],
            [0, datetime.datetime.fromisoformat("2021-11-06 00:00:00"), 500.0],
            [0, datetime.datetime.fromisoformat("2021-11-07 00:00:00"), 600.0],
            [0, datetime.datetime.fromisoformat("2021-11-08 00:00:00"), 700.0],
            [0, datetime.datetime.fromisoformat("2021-11-09 00:00:00"), 800.0],
            [0, datetime.datetime.fromisoformat("2021-11-10 00:00:00"), 900.0],
            [0, datetime.datetime.fromisoformat("2021-11-11 00:00:00"), 800.0],
            [0, datetime.datetime.fromisoformat("2021-11-12 00:00:00"), 700.0],
            [0, datetime.datetime.fromisoformat("2021-11-13 00:00:00"), 600.0],
            [0, datetime.datetime.fromisoformat("2021-11-14 00:00:00"), 500.0],
            [0, datetime.datetime.fromisoformat("2021-11-15 00:00:00"), 400.0],
            [0, datetime.datetime.fromisoformat("2021-11-16 00:00:00"), 300.0],
            [0, datetime.datetime.fromisoformat("2021-11-17 00:00:00"), 200.0],
            [0, datetime.datetime.fromisoformat("2021-11-18 00:00:00"), 100.0],
            [0, datetime.datetime.fromisoformat("2021-11-19 00:00:00"), 200.0],
            [0, datetime.datetime.fromisoformat("2021-11-20 00:00:00"), 300.0],
            [0, datetime.datetime.fromisoformat("2021-11-21 00:00:00"), 400.0],
            [0, datetime.datetime.fromisoformat("2021-11-22 00:00:00"), 500.0],
            [0, datetime.datetime.fromisoformat("2021-11-23 00:00:00"), 600.0],
            [0, datetime.datetime.fromisoformat("2021-11-24 00:00:00"), 700.0],
            [0, datetime.datetime.fromisoformat("2021-11-25 00:00:00"), 800.0],
            [0, datetime.datetime.fromisoformat("2021-11-26 00:00:00"), 900.0],
            [0, datetime.datetime.fromisoformat("2021-11-27 00:00:00"), 800.0],
            [0, datetime.datetime.fromisoformat("2021-11-28 00:00:00"), 700.0],
            [0, datetime.datetime.fromisoformat("2021-11-29 00:00:00"), 600.0],
            [0, datetime.datetime.fromisoformat("2021-11-30 00:00:00"), 500.0],
            [0, datetime.datetime.fromisoformat("2021-12-01 00:00:00"), 400.0],
            [0, datetime.datetime.fromisoformat("2021-12-02 00:00:00"), 300.0],
            [0, datetime.datetime.fromisoformat("2021-12-03 00:00:00"), 200.0]
        ])
        
        batch_source = dataframeToOperator(data, schemaStr='id int, ts timestamp, series double', op_type='batch')
        
        stream_source = dataframeToOperator(data, schemaStr='id int, ts timestamp, series double', op_type='stream')
        
        lstNetTrainBatchOp = LSTNetTrainBatchOp()\
            .setTimeCol("ts")\
            .setSelectedCol("series")\
            .setNumEpochs(10)\
            .setWindow(24)\
            .setHorizon(1)\
            .linkFrom(batch_source)
        
        overCountWindowStreamOp = OverCountWindowStreamOp()\
            .setClause("MTABLE_AGG_PRECEDING(ts, series) as mtable_agg_series")\
            .setTimeCol("ts")\
            .setPrecedingRows(24)
        
        lstNetPredictStreamOp = LSTNetPredictStreamOp(lstNetTrainBatchOp)\
            .setPredictNum(1)\
            .setPredictionCol("pred")\
            .setReservedCols([])\
            .setValueCol("mtable_agg_series")
        
        lstNetPredictStreamOp\
            .linkFrom(
                overCountWindowStreamOp\
                .linkFrom(stream_source)\
                .filter("ts = TO_TIMESTAMP('2021-12-03 00:00:00')")
            )\
            .print();
        
        StreamOperator.execute();
        pass