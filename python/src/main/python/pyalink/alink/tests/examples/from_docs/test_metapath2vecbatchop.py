import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestMetaPath2VecBatchOp(unittest.TestCase):
    def test_metapath2vecbatchop(self):

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
        
        df2 = pd.DataFrame([
            ["Bob", "A"],
            ["Bella", "A"],
            ["Karry", "A"],
            ["Lucy", "B"],
            ["Alice", "B"],
            ["Lisa", "B"],
            ["Karry", "B"]
        ])
        
        type = BatchOperator.fromDataframe(df2, schemaStr="node string, type string")
        
        metapathBatchOp = MetaPath2VecBatchOp() \
            .setSourceCol("start") \
            .setTargetCol("end") \
            .setWeightCol("value") \
            .setVertexCol("node") \
            .setTypeCol("type") \
            .setMetaPath("ABA") \
            .setWalkNum(2) \
            .setWalkLength(2) \
            .setMinCount(1) \
            .setVectorSize(4)
        metapathBatchOp.linkFrom(source, type).print()
        pass