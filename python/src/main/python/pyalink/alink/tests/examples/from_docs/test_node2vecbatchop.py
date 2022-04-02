import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestNode2VecBatchOp(unittest.TestCase):
    def test_node2vecbatchop(self):

        df = pd.DataFrame([
            ["Bob", "Lucy", 1.],
            ["Lucy", "Bob", 1.],
            ["Lucy", "Bella", 1.],
            ["Bella", "Lucy", 1.],
            ["Alice", "Lisa", 1.],
            ["Lisa", "Alice", 1.],
            ["Lisa", "Karry", 1.],
            ["Karry", "Lisa", 1.],
            ["Karry", "Bella", 1.],
            ["Bella", "Karry", 1.]
        ])
        source = BatchOperator.fromDataframe(df, schemaStr="start string, end string, value double")
        
        node2vecBatchOp = Node2VecBatchOp() \
          .setSourceCol("start")            \
          .setTargetCol("end")              \
          .setWeightCol("value")            \
          .setWalkNum(2)                    \
          .setWalkLength(2)                 \
          .setMinCount(1)                   \
          .setVectorSize(4)
        node2vecBatchOp.linkFrom(source).print()
        pass