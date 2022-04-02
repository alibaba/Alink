import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestHugeIndexerStringPredictBatchOp(unittest.TestCase):
    def test_hugeindexerstringpredictbatchop(self):

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
        
        data = BatchOperator.fromDataframe(df, schemaStr='f0 string, f1 string')
        
        stringindexer = StringIndexerTrainBatchOp()\
        			.setSelectedCol("f0")\
        			.setSelectedCols(["f1"])\
        			.setStringOrderType("alphabet_asc")
        
        predictor = HugeStringIndexerPredictBatchOp().setSelectedCols(["f0", "f1"])\
        			.setOutputCols(["f0_indexed", "f1_indexed"])
        		
        model = stringindexer.linkFrom(data)
        	
        result = predictor.linkFrom(model, data)
        		
        indexerString = HugeIndexerStringPredictBatchOp().setSelectedCols(["f0_indexed", "f1_indexed"])\
        			.setOutputCols(["f0_source", "f1_source"])
        		
        indexerString.linkFrom(model, result).print()
        pass