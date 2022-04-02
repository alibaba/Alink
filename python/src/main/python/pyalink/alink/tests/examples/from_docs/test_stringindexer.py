import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestStringIndexer(unittest.TestCase):
    def test_stringindexer(self):

        df_data = pd.DataFrame([
            ["football"],
            ["football"],
            ["football"],
            ["basketball"],
            ["basketball"],
            ["tennis"],
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr='f0 string')
        
        stringindexer = StringIndexer() \
            .setSelectedCol("f0") \
            .setOutputCol("f0_indexed") \
            .setStringOrderType("frequency_asc")
        
        stringindexer.fit(data).transform(data).print()
        pass