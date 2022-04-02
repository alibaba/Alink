import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestHugeWord2VecTrainBatchOp(unittest.TestCase):
    def test_hugeword2vectrainbatchop(self):

        tokens = pd.DataFrame([
            ["A B C"]
        ])
        
        source = BatchOperator.fromDataframe(tokens, schemaStr='tokens string')
        
        word2vecBatchOp = HugeWord2VecTrainBatchOp() \
          .setSelectedCol("tokens")         \
          .setMinCount(1)                   \
          .setVectorSize(4)
        word2vecBatchOp.linkFrom(source).print()
        pass