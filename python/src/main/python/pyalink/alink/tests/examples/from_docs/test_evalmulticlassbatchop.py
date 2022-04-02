import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestEvalMultiClassBatchOp(unittest.TestCase):
    def test_evalmulticlassbatchop(self):

        df = pd.DataFrame([
            ["prefix1", "{\"prefix1\": 0.9, \"prefix0\": 0.1}"],
            ["prefix1", "{\"prefix1\": 0.8, \"prefix0\": 0.2}"],
            ["prefix1", "{\"prefix1\": 0.7, \"prefix0\": 0.3}"],
            ["prefix0", "{\"prefix1\": 0.75, \"prefix0\": 0.25}"],
            ["prefix0", "{\"prefix1\": 0.6, \"prefix0\": 0.4}"]])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='label string, detailInput string')
        
        metrics = EvalMultiClassBatchOp().setLabelCol("label").setPredictionDetailCol("detailInput").linkFrom(inOp).collectMetrics()
        print("Prefix0 accuracy:", metrics.getAccuracy("prefix0"))
        print("Prefix1 recall:", metrics.getRecall("prefix1"))
        print("Macro Precision:", metrics.getMacroPrecision())
        print("Micro Recall:", metrics.getMicroRecall())
        print("Weighted Sensitivity:", metrics.getWeightedSensitivity())
        pass