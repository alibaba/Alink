import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestHugeLabeledWord2VecTrainBatchOp(unittest.TestCase):
    def test_hugelabeledword2vectrainbatchop(self):

        tokens = pd.DataFrame([
            ["Bob Lucy Bella"]
        ])
        
        nodeType = pd.DataFrame([
            ["Bob", "A"],
            ["Bella", "A"],
            ["Karry", "A"],
            ["Lucy", "B"],
            ["Alice", "B"],
            ["Lisa", "B"]
        ])
        
        source = BatchOperator.fromDataframe(tokens, schemaStr='tokens string')
        typed = BatchOperator.fromDataframe(nodeType, schemaStr='node string, type string')
        
        labeledWord2vecBatchOp = HugeLabeledWord2VecTrainBatchOp() \
          .setSelectedCol("tokens")         \
          .setVertexCol("node")             \
          .setTypeCol("type")               \
          .setMinCount(1)                   \
          .setVectorSize(4)
        labeledWord2vecBatchOp.linkFrom(source, typed).print()
        pass