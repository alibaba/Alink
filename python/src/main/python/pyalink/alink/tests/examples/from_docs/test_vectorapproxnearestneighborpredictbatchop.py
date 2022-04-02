import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorApproxNearestNeighborPredictBatchOp(unittest.TestCase):
    def test_vectorapproxnearestneighborpredictbatchop(self):

        df = pd.DataFrame([
            [0, "0 0 0"],
            [1, "1 1 1"],
            [2, "2 2 2"]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
        train = VectorApproxNearestNeighborTrainBatchOp().setIdCol("id").setSelectedCol("vec").linkFrom(inOp)
        predict = VectorApproxNearestNeighborPredictBatchOp().setSelectedCol("vec").setTopN(3).linkFrom(train, inOp)
        predict.print()
        pass