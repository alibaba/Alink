import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestWord2VecPredictBatchOp(unittest.TestCase):
    def test_word2vecpredictbatchop(self):

        df = pd.DataFrame([
            ["A B C"]
        ])
        
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='tokens string')
        inOp2 = StreamOperator.fromDataframe(df, schemaStr='tokens string')
        train = Word2VecTrainBatchOp().setSelectedCol("tokens").setMinCount(1).setVectorSize(4).linkFrom(inOp1)
        predictBatch = Word2VecPredictBatchOp().setSelectedCol("tokens").linkFrom(train, inOp1)
        
        train.lazyPrint(-1)
        predictBatch.print()
        
        predictStream = Word2VecPredictStreamOp(train).setSelectedCol("tokens").linkFrom(inOp2)
        predictStream.print()
        StreamOperator.execute()
        pass