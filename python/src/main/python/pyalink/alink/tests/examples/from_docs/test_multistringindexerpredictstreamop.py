import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestMultiStringIndexerPredictStreamOp(unittest.TestCase):
    def test_multistringindexerpredictstreamop(self):

        df_data = pd.DataFrame([
            ["football", "apple"],
            ["football", "banana"],
            ["football", "banana"],
            ["basketball", "orange"],
            ["basketball", "grape"],
            ["tennis", "grape"],
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr='f0 string,f1 string')
        
        stream_data = StreamOperator.fromDataframe(df_data, schemaStr='f0 string,f1 string')
        
        stringindexer = MultiStringIndexerTrainBatchOp() \
             .setSelectedCols(["f0", "f1"]) \
             .setStringOrderType("frequency_asc")
        
        model = stringindexer.linkFrom(data)
        
        predictor = MultiStringIndexerPredictStreamOp(model)\
             .setSelectedCols(["f0", "f1"])\
             .setOutputCols(["f0_indexed", "f1_indexed"])
        
        predictor.linkFrom(stream_data).print()
        
        StreamOperator.execute()
        pass