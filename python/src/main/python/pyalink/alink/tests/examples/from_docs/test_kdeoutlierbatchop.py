import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestKdeOutlierBatchOp(unittest.TestCase):
    def test_kdeoutlierbatchop(self):

        import pandas as pd
        df = pd.DataFrame([
                        [-1.1],
                        [0.2],
                        [101.1],
                        [0.3]
                ])
        
        dataOp = BatchOperator.fromDataframe(df, schemaStr='val double')
        
        outlierOp = KdeOutlierBatchOp()\
        			.setFeatureCols(["val"])\
        			.setBandwidth(4.0)\
        			.setOutlierThreshold(15.0)\
        			.setPredictionCol("pred")\
        			.setPredictionDetailCol("pred_detail")
        
        dataOp.link(outlierOp).print()
        pass