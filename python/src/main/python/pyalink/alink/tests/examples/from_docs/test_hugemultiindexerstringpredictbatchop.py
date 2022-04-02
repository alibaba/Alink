import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestHugeMultiIndexerStringPredictBatchOp(unittest.TestCase):
    def test_hugemultiindexerstringpredictbatchop(self):

        df = pd.DataFrame([
            [1, "football", "apple"],
            [2, "football", "apple"],
            [3, "football", "apple"],
            [4, "basketball", "apple"],
            [5, "basketball", "apple"],
            [6, "tennis", "pair"],
            [7, "tennis", "pair"],
            [8, "pingpang", "banana"],
            [9, "pingpang", "banana"],
            [0, "baseball", "banana"]
        ])
        
        data = BatchOperator.fromDataframe(df, schemaStr='id long, f0 string, f1 string')
        
        stringindexer = MultiStringIndexerTrainBatchOp()\
            .setSelectedCols(["f0", "f1"])\
            .setStringOrderType("frequency_asc")
        
        model = stringindexer.linkFrom(data)
        
        predictor = HugeMultiStringIndexerPredictBatchOp()\
            .setSelectedCols(["f0", "f1"])
        result = predictor.linkFrom(model, data)
        
        stringPredictor = HugeMultiIndexerStringPredictBatchOp()\
            .setSelectedCols(["f0", "f1"])\
            .setOutputCols(["f0_source", "f1_source"])
        stringPredictor.linkFrom(model, result).print();
        pass