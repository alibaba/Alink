import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestMultiStringIndexer(unittest.TestCase):
    def test_multistringindexer(self):

        df_data = pd.DataFrame([
            ["football"],
            ["football"],
            ["football"],
            ["basketball"],
            ["basketball"],
            ["tennis"],
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr='f0 string')
        
        stringindexer = MultiStringIndexer() \
            .setSelectedCols(["f0"]) \
            .setOutputCols(["f0_indexed"]) \
            .setStringOrderType("frequency_asc")
        
        stringindexer.fit(data).transform(data).print()
        pass