import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPinjiu(unittest.TestCase):

    def test_stop_word_remover(self):
        data = np.array([
            [0, u'二手旧书:医学电磁成像'],
            [1, u'二手美国文学选读（ 下册 ）李宜燮南开大学出版社 9787310003969'],
            [2, u'二手正版图解象棋入门/谢恩思主编/华龄出版社'],
            [3, u'二手中国糖尿病文献索引'],
            [4, u'二手郁达夫文集（ 国内版 ）全十二册馆藏书']
        ])

        df = pd.DataFrame({"id": data[:, 0], "text": data[:, 1]})
        inOp = dataframeToOperator(df, schemaStr='id long, text string', op_type='batch')

        pipeline = (
            Pipeline()
                .add(Segment().setSelectedCol("text"))
                .add(StopWordsRemover().setSelectedCol("text"))
        )
        pipeline.fit(inOp).transform(inOp).print()
