import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestMultiHotPredictStreamOp(unittest.TestCase):
    def test_multihotpredictstreamop(self):

        df = pd.DataFrame([
            ["a b", 1],
            ["b c", 1],
            ["c d", 1],
            ["a d", 2],
            ["d e", 2],
            [None, 1]
        ])
        
        # load data
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='query string, weight long')
        streamOp = StreamOperator.fromDataframe(df, schemaStr='query string, weight long')
        
        # multi hot train
        multi_hot = MultiHotTrainBatchOp().setSelectedCols(["query"])
        model = inOp.link(multi_hot)
        model.print()
        
        # batch predict
        predictor = MultiHotPredictStreamOp(model).setSelectedCols(["query"]).setOutputCols(["output"])
        predictor.linkFrom(streamOp).print()
        StreamOperator.execute()
        pass