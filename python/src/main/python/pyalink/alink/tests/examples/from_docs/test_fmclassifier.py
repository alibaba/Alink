import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestFmClassifier(unittest.TestCase):
    def test_fmclassifier(self):

        df = pd.DataFrame([
               ["1:1.1 3:2.0", 1.0],
               ["2:2.1 10:3.1", 1.0],
               ["1:1.2 5:3.2", 0.0],
               ["3:1.2 7:4.2", 0.0]
        ])
        
        input = BatchOperator.fromDataframe(df, schemaStr='kv string, label double')
        test = StreamOperator.fromDataframe(df, schemaStr='kv string, label double')
        # load data
        fm = FmClassifier().setVectorCol("kv").setLabelCol("label").setPredictionCol("pred")
        model = fm.fit(input)
        model.transform(test).print()
        StreamOperator.execute()
        pass