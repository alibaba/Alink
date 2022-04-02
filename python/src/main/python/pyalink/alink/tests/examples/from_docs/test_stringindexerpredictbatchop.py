import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestStringIndexerPredictBatchOp(unittest.TestCase):
    def test_stringindexerpredictbatchop(self):

        df = pd.DataFrame([
            ["football"],
            ["football"],
            ["football"],
            ["basketball"],
            ["basketball"],
            ["tennis"],
        ])
        
        
        data = BatchOperator.fromDataframe(df, schemaStr='f0 string')
        
        stringindexer = StringIndexerTrainBatchOp() \
            .setSelectedCol("f0") \
            .setStringOrderType("frequency_asc")
        
        predictor = StringIndexerPredictBatchOp().setSelectedCol("f0").setOutputCol("f0_indexed")
        
        model = stringindexer.linkFrom(data)
        predictor.linkFrom(model, data).print()
        pass