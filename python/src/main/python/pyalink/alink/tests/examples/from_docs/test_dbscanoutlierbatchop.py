import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestDbscanOutlierBatchOp(unittest.TestCase):
    def test_dbscanoutlierbatchop(self):

        import pandas as pd
        df = pd.DataFrame([
                       [1.0],
                       [1.0],
                       [1.0],
                       [1.0],
                       [1.0],
                       [6.0]
                ])
        source = BatchOperator.fromDataframe(df, schemaStr='f0 double')
        
        DbscanOutlierBatchOp()\
        			.setFeatureCols(["f0"])\
        			.setPredictionCol("outlier")\
        			.setPredictionDetailCol("details")\
        			.setMinPoints(4)\
        			.linkFrom(source)\
        			.print()
            
        pass