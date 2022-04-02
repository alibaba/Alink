import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestMultiStringIndexerPredictBatchOp(unittest.TestCase):
    def test_multistringindexerpredictbatchop(self):

        df = pd.DataFrame([
            ["football", "apple"],
            ["football", "banana"],
            ["football", "banana"],
            ["basketball", "orange"],
            ["basketball", "grape"],
            ["tennis", "grape"],
        ])
        
        data = BatchOperator.fromDataframe(df, schemaStr='f0 string,f1 string')
        
        stringindexer = MultiStringIndexerTrainBatchOp() \
            .setSelectedCols(["f0", "f1"]) \
            .setStringOrderType("frequency_asc")
        
        predictor = MultiStringIndexerPredictBatchOp().setSelectedCols(["f0", "f1"]).setOutputCols(["f0_indexed", "f1_indexed"])
        
        model = stringindexer.linkFrom(data)
        predictor.linkFrom(model, data).print()
        pass