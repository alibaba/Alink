import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestKModes(unittest.TestCase):
    def test_kmodes(self):

        df = pd.DataFrame([
            ["pc", "Hp.com"],
            ["camera", "Hp.com"],
            ["digital camera", "Hp.com"],
            ["camera", "BestBuy.com"],
            ["digital camera", "BestBuy.com"],
            ["tv", "BestBuy.com"],
            ["flower", "Teleflora.com"],
            ["flower", "Orchids.com"]
        ])
        
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='f0 string, f1 string')
        
        kmodes = KModes()\
            .setFeatureCols(["f0", "f1"])\
            .setK(2)\
            .setPredictionCol("pred")
            
        kmodes.fit(inOp1).transform(inOp1).print()
        pass