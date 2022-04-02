import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestSegmentStreamOp(unittest.TestCase):
    def test_segmentstreamop(self):

        df = pd.DataFrame([
            [0, u'二手旧书:医学电磁成像'],
            [1, u'二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969'],
            [2, u'二手正版图解象棋入门/谢恩思主编/华龄出版社'],
            [3, u'二手中国糖尿病文献索引'],
            [4, u'二手郁达夫文集（ 国内版 ）全十二册馆藏书']
        ])
        
        inOp = StreamOperator.fromDataframe(df, schemaStr='id long, text string')
        
        SegmentStreamOp() \
        .setSelectedCol("text") \
        .setOutputCol("segment") \
        .linkFrom(inOp).print()
        
        StreamOperator.execute()
        pass