import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestEvalMultiClassStreamOp(unittest.TestCase):
    def test_evalmulticlassstreamop(self):

        df = pd.DataFrame([
            ["prefix1", "{\"prefix1\": 0.9, \"prefix0\": 0.1}"],
            ["prefix1", "{\"prefix1\": 0.8, \"prefix0\": 0.2}"],
            ["prefix1", "{\"prefix1\": 0.7, \"prefix0\": 0.3}"],
            ["prefix0", "{\"prefix1\": 0.75, \"prefix0\": 0.25}"],
            ["prefix0", "{\"prefix1\": 0.6, \"prefix0\": 0.4}"]
        ])
        
        inOp = StreamOperator.fromDataframe(df, schemaStr='label string, detailInput string')
        
        EvalMultiClassStreamOp().setLabelCol("label").setPredictionDetailCol("detailInput").setTimeInterval(0.001).linkFrom(inOp).print()
        StreamOperator.execute()
        pass