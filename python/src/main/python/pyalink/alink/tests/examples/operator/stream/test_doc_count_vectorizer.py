import unittest

from pyalink.alink import *


class TestPinjiu(unittest.TestCase):

    def test_doc_count_vectorizer_op(self):
        import numpy as np
        import pandas as pd
        data = np.array([
            [0, u'二手旧书:医学电磁成像'],
            [1, u'二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969'],
            [2, u'二手正版图解象棋入门/谢恩思主编/华龄出版社'],
            [3, u'二手中国糖尿病文献索引'],
            [4, u'二手郁达夫文集（ 国内版 ）全十二册馆藏书']])
        df = pd.DataFrame({"id": data[:, 0], "text": data[:, 1]})
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='id int, text string')
        inOp2 = StreamOperator.fromDataframe(df, schemaStr='id int, text string')

        segment = SegmentBatchOp().setSelectedCol("text").linkFrom(inOp1)
        train = DocCountVectorizerTrainBatchOp().setSelectedCol("text").linkFrom(segment)
        predictBatch = DocCountVectorizerPredictBatchOp().setSelectedCol("text").linkFrom(train, segment)
        [model, predict] = collectToDataframes(train, predictBatch)
        print(model)
        print(predict)

        segment = SegmentStreamOp().setSelectedCol("text").linkFrom(inOp2)
        predictStream = DocCountVectorizerPredictStreamOp(train).setSelectedCol("text").linkFrom(segment)
        predictStream.print(refreshInterval=-1)
        StreamOperator.execute()
