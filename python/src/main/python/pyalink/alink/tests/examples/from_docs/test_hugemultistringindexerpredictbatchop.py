import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestHugeMultiStringIndexerPredictBatchOp(unittest.TestCase):
    def test_hugemultistringindexerpredictbatchop(self):

        df = pd.DataFrame([
            ["a", 1], ["b", 2], ["b", 3], ["c", 4]
        ])
        
        op = BatchOperator.fromDataframe(df, schemaStr='f0 string, f1 int')
        
        stringIndexer = MultiStringIndexerTrainBatchOp().setSelectedCols(["f1", "f0"]).setStringOrderType("frequency_desc")
        stringIndexer.linkFrom(op)
        
        predictor = HugeMultiStringIndexerPredictBatchOp().setSelectedCols(["f0"]).setReservedCols(["f0", "f1"])\
            .setOutputCols(["f0_index"]).setHandleInvalid("skip");
        predictor.linkFrom(stringIndexer, op).print()
        pass