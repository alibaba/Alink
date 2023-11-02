import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestGroupDbscanBatchOp(unittest.TestCase):
    def test_groupdbscanbatchop(self):

        df = pd.DataFrame([
            [0, "id_1", 2.0, 3.0],
            [0, "id_2", 2.1, 3.1],
            [0, "id_18", 2.4, 3.2],
            [0, "id_15", 2.8, 3.2],
            [0, "id_12", 2.1, 3.1],
            [0, "id_3", 200.1, 300.1],
            [0, "id_4", 200.2, 300.2],
            [0, "id_8", 200.6, 300.6],
        
            [1, "id_5", 200.3, 300.3],
            [1, "id_6", 200.4, 300.4],
            [1, "id_7", 200.5, 300.5],
            [1, "id_16", 300., 300.2],
            [1, "id_9", 2.1, 3.1],
            [1, "id_10", 2.2, 3.2],
            [1, "id_11", 2.3, 3.3],
            [1, "id_13", 2.4, 3.4],
            [1, "id_14", 2.5, 3.5],
            [1, "id_17", 2.6, 3.6],
            [1, "id_19", 2.7, 3.7],
            [1, "id_20", 2.8, 3.8],
            [1, "id_21", 2.9, 3.9],
        
            [2, "id_20", 2.8, 3.8]])
        
        source = BatchOperator.fromDataframe(df, schemaStr='group string, id string, c1 double, c2 double')
        
        groupDbscan = GroupDbscanBatchOp()\
            .setIdCol("id")\
            .setGroupCols(["group"])\
            .setFeatureCols(["c1", "c2"])\
            .setMinPoints(4)\
            .setEpsilon(0.6)\
            .linkFrom(source)
        
        groupDbscan.print()
        pass