import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestTumbleTimeWindowStreamOp(unittest.TestCase):
    def test_tumbletimewindowstreamop(self):

        sourceFrame = pd.DataFrame([
                [0, 0, 0, 1],
                [0, 2, 0, 2],
                [0, 1, 1, 3],
                [0, 3, 1, 4],
                [0, 3, 3, 5],
                [0, 0, 3, 6],
                [0, 0, 4, 7],
                [0, 3, 4, 8],
                [0, 1, 2, 9],
                [0, 2, 2, 10],
            ])
        
        streamSource = StreamOperator.fromDataframe(sourceFrame,schemaStr="user int, device long, ip long, timeCol long")
        
        op = SessionTimeWindowStreamOp().setTimeCol("timeCol").setSessionGapTime(60).setLatency(180).setPartitionCols(["user"]).setClause("count_preceding(ip) as countip")
        
        streamSource.select('user, device, ip, to_timestamp(timeCol) as timeCol').link(op).print()
        
        op2 = TumbleTimeWindowStreamOp().setTimeCol("timeCol").setWindowTime(60).setPartitionCols(["user"]).setClause("count_preceding(ip) as countip")
                                                    
        streamSource.select('user, device, ip, to_timestamp(timeCol) as timeCol').link(op2).print()
        
        StreamOperator.execute()
        
        pass