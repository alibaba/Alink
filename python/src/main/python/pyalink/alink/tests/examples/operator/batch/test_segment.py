import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def run_segment(self):
        # -*- coding=UTF-8 -*-

        data = np.array([
            [0, u'二手旧书:医学电磁成像'],
            [1, u'二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969'],
            [2, u'二手正版图解象棋入门/谢恩思主编/华龄出版社'],
            [3, u'二手中国糖尿病文献索引'],
            [4, u'二手郁达夫文集（ 国内版 ）全十二册馆藏书']
        ])

        df = pd.DataFrame({"id": data[:, 0], "text": data[:, 1]})
        inOp = dataframeToOperator(df, schemaStr='id long, text string', op_type='batch')

        segment = SegmentBatchOp().setSelectedCol("text").setOutputCol("segment")

        BatchOperator.collectToDataframe(segment.linkFrom(inOp))
