import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestStratifiedSampleWithSizeBatchOp(unittest.TestCase):
    def test_stratifiedsamplewithsizebatchop(self):

        df = pd.DataFrame([
                ['a',0.0,0.0],
                ['a',0.2,0.1],
                ['b',0.2,0.8],
                ['b',9.5,9.7],
                ['b',9.1,9.6],
                ['b',9.3,9.9]
            ])
        
        
        batchData = BatchOperator.fromDataframe(df, schemaStr='x1 string, x2 double, x3 double')
        sampleOp = StratifiedSampleWithSizeBatchOp() \
               .setStrataCol("x1") \
               .setStrataSizes("a:1,b:2")
        
        batchData.link(sampleOp).print()
        pass