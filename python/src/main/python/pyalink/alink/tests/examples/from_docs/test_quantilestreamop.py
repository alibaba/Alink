import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestQuantileStreamOp(unittest.TestCase):
    def test_quantilestreamop(self):

        df_data = pd.DataFrame([
                [0.0,0.0,0.0],
                [0.1,0.2,0.1],
                [0.2,0.2,0.8],
                [9.0,9.5,9.7],
                [9.1,9.1,9.6],
                [9.2,9.3,9.9]
                ])
        
        
        streamData = StreamOperator.fromDataframe(df_data, schemaStr='x1 double, x2 double, x3 double')
        
        
        quanOp = QuantileStreamOp()\
            .setSelectedCols(["x2","x3"])\
            .setQuantileNum(5)
        
        #control data speed, 1 per second.      
        speedControl = SpeedControlStreamOp()\
            .setTimeInterval(.3)
        
        streamData.link(speedControl).link(quanOp).print()
        
        StreamOperator.execute()
        pass