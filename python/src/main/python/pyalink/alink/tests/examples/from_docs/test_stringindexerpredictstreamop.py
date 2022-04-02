import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestStringIndexerPredictStreamOp(unittest.TestCase):
    def test_stringindexerpredictstreamop(self):

        df_data = pd.DataFrame([
            ["football"],
            ["football"],
            ["football"],
            ["basketball"],
            ["basketball"],
            ["tennis"],
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr='f0 string')
        
        stream_data = StreamOperator.fromDataframe(df_data, schemaStr='f0 string')
        
        stringindexer = StringIndexerTrainBatchOp() \
            .setSelectedCol("f0") \
            .setStringOrderType("frequency_asc")
        
        model = stringindexer.linkFrom(data)
        
        predictor = StringIndexerPredictStreamOp(model)\
                        .setSelectedCol("f0")\
                        .setOutputCol("f0_indexed")
        
        predictor.linkFrom(stream_data).print()
        
        StreamOperator.execute()
        
        pass