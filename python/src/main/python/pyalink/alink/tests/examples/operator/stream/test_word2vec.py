import unittest

from pyalink.alink import *


class TestPinjiu(unittest.TestCase):

    def test_word2vec_op(self):
        import numpy as np
        import pandas as pd

        data = np.array([
            ["A B C"]
        ])

        df = pd.DataFrame({"tokens": data[:, 0]})
        inOp1 = dataframeToOperator(df, schemaStr='tokens string', op_type='batch')
        inOp2 = dataframeToOperator(df, schemaStr='tokens string', op_type='stream')
        train = Word2VecTrainBatchOp().setSelectedCol("tokens").setMinCount(1).setVectorSize(4).linkFrom(inOp1)
        predictBatch = Word2VecPredictBatchOp().setSelectedCol("tokens").linkFrom(train, inOp1)

        [model, predict] = collectToDataframes(train, predictBatch)
        print(model)
        print(predict)

        predictStream = Word2VecPredictStreamOp(train).setSelectedCol("tokens").linkFrom(inOp2)
        predictStream.print(refreshInterval=-1)
        StreamOperator.execute()
