import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestWordCountBatchOp(unittest.TestCase):
    def test_wordcountbatchop(self):

        df = pd.DataFrame([
            ["doc0", "中国 的 文化"],
            ["doc1", "只要 功夫 深"],
            ["doc2", "北京 的 拆迁"],
            ["doc3", "人名 的 名义"]
        ])
        
        source = BatchOperator.fromDataframe(df, "id string, content string")
        wordCountBatchOp = WordCountBatchOp()\
            .setSelectedCol("content")\
            .setWordDelimiter(" ")\
            .linkFrom(source)
        wordCountBatchOp.print()
        pass