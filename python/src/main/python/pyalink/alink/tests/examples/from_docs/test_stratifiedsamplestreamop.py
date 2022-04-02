import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestStratifiedSampleStreamOp(unittest.TestCase):
    def test_stratifiedsamplestreamop(self):

        df_data = pd.DataFrame([
                ['a',0.0,0.0],
                ['a',0.2,0.1],
                ['b',0.2,0.8],
                ['b',9.5,9.7],
                ['b',9.1,9.6],
                ['b',9.3,9.9]
            ])
        
        streamData = StreamOperator.fromDataframe(df_data, schemaStr='x1 string, x2 double, x3 double')
        sampleStreamOp = StratifiedSampleStreamOp()\
               .setStrataCol("x1")\
               .setStrataRatios("a:0.5,b:0.5")
        
        sampleStreamOp.linkFrom(streamData).print()
        
        StreamOperator.execute()
        pass