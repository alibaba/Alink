import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestEvalClusterBatchOp(unittest.TestCase):
    def test_evalclusterbatchop(self):

        df = pd.DataFrame([
            [0, "0 0 0"],
            [0, "0.1,0.1,0.1"],
            [0, "0.2,0.2,0.2"],
            [1, "9 9 9"],
            [1, "9.1 9.1 9.1"],
            [1, "9.2 9.2 9.2"]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
        
        metrics = EvalClusterBatchOp().setVectorCol("vec").setPredictionCol("id").linkFrom(inOp).collectMetrics()
        
        print("Total Samples Number:", metrics.getCount())
        print("Cluster Number:", metrics.getK())
        print("Cluster Array:", metrics.getClusterArray())
        print("Cluster Count Array:", metrics.getCountArray())
        print("CP:", metrics.getCp())
        print("DB:", metrics.getDb())
        print("SP:", metrics.getSp())
        print("SSB:", metrics.getSsb())
        print("SSW:", metrics.getSsw())
        print("CH:", metrics.getVrc())
        pass