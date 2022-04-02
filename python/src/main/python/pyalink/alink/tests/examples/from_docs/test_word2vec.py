import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestWord2Vec(unittest.TestCase):
    def test_word2vec(self):

        df = pd.DataFrame([
            ["A B C"]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='tokens string')
        word2vec = Word2Vec().setSelectedCol("tokens").setMinCount(1).setVectorSize(4)
        word2vec.fit(inOp).transform(inOp).print()
        pass