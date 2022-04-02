import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestMultiHotPredictBatchOp(unittest.TestCase):
    def test_multihotpredictbatchop(self):

        # load data
        df = pd.DataFrame([
            ["a b", 1],
            ["b c", 1],
            ["c d", 1],
            ["a d", 2],
            ["d e", 2],
            [None, 1]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='query string, weight long')
        
        # multi hot train
        multi_hot = MultiHotTrainBatchOp().setSelectedCols(["query"])
        model = inOp.link(multi_hot)
        model.print()
        
        # batch predict
        predictor = MultiHotPredictBatchOp().setSelectedCols(["query"]).setOutputCols(["output"])
        print(BatchOperator.collectToDataframe(predictor.linkFrom(model, inOp)))
        pass