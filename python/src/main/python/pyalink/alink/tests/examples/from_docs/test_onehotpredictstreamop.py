import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestOneHotPredictStreamOp(unittest.TestCase):
    def test_onehotpredictstreamop(self):

        df = pd.DataFrame([
            ["a", 1],
            ["b", 1],
            ["c", 1],
            ["e", 2],
            ["a", 2],
            ["b", 1],
            ["c", 2],
            ["d", 2],
            [None, 1]
        ])
        
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='query string, weight long')
        sinOp = StreamOperator.fromDataframe(df, schemaStr='query string, weight long')
        
        # one hot train
        one_hot = OneHotTrainBatchOp().setSelectedCols(["query"])
        model = inOp.link(one_hot)
        model.lazyPrint(10)
        
        # batch predict
        predictor = OneHotPredictBatchOp().setOutputCols(["output"])
        predictor.linkFrom(model, inOp).print()
        
        # stream predict
        spredictor = OneHotPredictStreamOp(model).setOutputCols(["output"])
        spredictor.linkFrom(sinOp).print()
        
        StreamOperator.execute()
        
        pass