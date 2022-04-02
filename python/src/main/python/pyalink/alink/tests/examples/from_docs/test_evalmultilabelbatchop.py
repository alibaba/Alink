import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestEvalMultiLabelBatchOp(unittest.TestCase):
    def test_evalmultilabelbatchop(self):

        df = pd.DataFrame([
            ["{\"object\":\"[0.0, 1.0]\"}", "{\"object\":\"[0.0, 2.0]\"}"],
            ["{\"object\":\"[0.0, 2.0]\"}", "{\"object\":\"[0.0, 1.0]\"}"],
            ["{\"object\":\"[]\"}", "{\"object\":\"[0.0]\"}"],
            ["{\"object\":\"[2.0]\"}", "{\"object\":\"[2.0]\"}"],
            ["{\"object\":\"[2.0, 0.0]\"}", "{\"object\":\"[2.0, 0.0]\"}"],
            ["{\"object\":\"[0.0, 1.0, 2.0]\"}", "{\"object\":\"[0.0, 1.0]\"}"],
            ["{\"object\":\"[1.0]\"}", "{\"object\":\"[1.0, 2.0]\"}"]
        ])
        
        source = BatchOperator.fromDataframe(df, "pred string, label string")
        
        evalMultiLabelBatchOp: EvalMultiLabelBatchOp = EvalMultiLabelBatchOp().setLabelCol("label").setPredictionCol("pred").linkFrom(source)
        metrics = evalMultiLabelBatchOp.collectMetrics()
        print(metrics)
        pass