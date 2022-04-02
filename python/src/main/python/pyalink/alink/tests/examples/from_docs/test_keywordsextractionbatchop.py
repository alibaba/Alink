import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestKeywordsExtractionBatchOp(unittest.TestCase):
    def test_keywordsextractionbatchop(self):

        df = pd.DataFrame([
            [0, u'二手旧书:医学电磁成像'],
            [1, u'二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969'],
            [2, u'二手正版图解象棋入门/谢恩思主编/华龄出版社'],
            [3, u'二手中国糖尿病文献索引'],
            [4, u'二手郁达夫文集（ 国内版 ）全十二册馆藏书']
        ])
        
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='id int, text string')
        inOp2 = StreamOperator.fromDataframe(df, schemaStr='id int, text string')
        
        segment = SegmentBatchOp().setSelectedCol("text").linkFrom(inOp1)
        remover = StopWordsRemoverBatchOp().setSelectedCol("text").linkFrom(segment)
        keywords = KeywordsExtractionBatchOp().setSelectedCol("text").setMethod("TF_IDF").setTopN(3).linkFrom(remover)
        keywords.print()
        
        segment2 = SegmentStreamOp().setSelectedCol("text").linkFrom(inOp2)
        remover2 = StopWordsRemoverStreamOp().setSelectedCol("text").linkFrom(segment2)
        keywords2 = KeywordsExtractionStreamOp().setSelectedCol("text").setTopN(3).linkFrom(remover2)
        keywords2.print()
        StreamOperator.execute()
        pass