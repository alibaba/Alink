import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestMultiStringIndexerTrainBatchOp(unittest.TestCase):
    def test_multistringindexertrainbatchop(self):

        df = pd.DataFrame([
            ["football"],
            ["football"],
            ["football"],
            ["basketball"],
            ["basketball"],
            ["tennis"],
        ])
        
        
        data = BatchOperator.fromDataframe(df, schemaStr='f0 string')
        
        stringindexer = MultiStringIndexerTrainBatchOp() \
            .setSelectedCols(["f0"]) \
            .setStringOrderType("frequency_asc")
        
        model = stringindexer.linkFrom(data)
        model.print()
        pass