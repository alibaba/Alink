import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestIndexToStringPredictBatchOp(unittest.TestCase):
    def test_indextostringpredictbatchop(self):

        df = pd.DataFrame([
            ["football"],
            ["football"],
            ["football"],
            ["basketball"],
            ["basketball"],
            ["tennis"],
        ])
        
        
        data = BatchOperator.fromDataframe(df, schemaStr='f0 string')
        
        stringIndexer = StringIndexer() \
            .setModelName("string_indexer_model") \
            .setSelectedCol("f0") \
            .setOutputCol("f0_indexed") \
            .setStringOrderType("frequency_asc")
        
        indexed = stringIndexer.fit(data).transform(data)
        
        indexed.print()
        pass