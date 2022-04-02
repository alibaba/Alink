import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestStringIndexerTrainBatchOp(unittest.TestCase):
    def test_stringindexertrainbatchop(self):

        df = pd.DataFrame([
            ["football", "apple"],
            ["football", "apple"],
            ["football", "apple"],
            ["basketball", "apple"],
            ["basketball", "apple"],
            ["tennis", "pair"],
            ["tennis", "pair"],
            ["pingpang", "banana"],
            ["pingpang", "banana"],
            ["baseball", "banana"]
        ])
        
        
        data = BatchOperator.fromDataframe(df, schemaStr='f0 string,f1 string')
        
        stringindexer = StringIndexerTrainBatchOp() \
            .setSelectedCol("f0") \
            .setSelectedCols(["f1"]) \
            .setStringOrderType("alphabet_asc")
        
        model = stringindexer.linkFrom(data)
        model.print()
        pass