import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestEvalBinaryClassBatchOp(unittest.TestCase):
    def test_evalbinaryclassbatchop(self):

        df = pd.DataFrame([
            ["prefix1", "{\"prefix1\": 0.9, \"prefix0\": 0.1}"],
            ["prefix1", "{\"prefix1\": 0.8, \"prefix0\": 0.2}"],
            ["prefix1", "{\"prefix1\": 0.7, \"prefix0\": 0.3}"],
            ["prefix0", "{\"prefix1\": 0.75, \"prefix0\": 0.25}"],
            ["prefix0", "{\"prefix1\": 0.6, \"prefix0\": 0.4}"]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='label string, detailInput string')
        
        metrics = EvalBinaryClassBatchOp().setLabelCol("label").setPredictionDetailCol("detailInput").linkFrom(inOp).collectMetrics()
        print("AUC:", metrics.getAuc())
        print("KS:", metrics.getKs())
        print("PRC:", metrics.getPrc())
        print("Accuracy:", metrics.getAccuracy())
        print("Macro Precision:", metrics.getMacroPrecision())
        print("Micro Recall:", metrics.getMicroRecall())
        print("Weighted Sensitivity:", metrics.getWeightedSensitivity())
        pass