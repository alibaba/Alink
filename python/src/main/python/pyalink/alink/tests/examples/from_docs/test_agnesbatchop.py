import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestAgnesBatchOp(unittest.TestCase):
    def test_agnesbatchop(self):

        df = pd.DataFrame([
                ["id_1", "2.0,3.0"],
                ["id_2", "2.1,3.1"],
                ["id_3", "200.1,300.1"],
                ["id_4", "200.2,300.2"],
                ["id_5", "200.3,300.3"],
                ["id_6", "200.4,300.4"],
                ["id_7", "200.5,300.5"],
                ["id_8", "200.6,300.6"],
                ["id_9", "2.1,3.1"],
                ["id_10", "2.1,3.1"],
                ["id_11", "2.1,3.1"],
                ["id_12", "2.1,3.1"],
                ["id_16", "300.,3.2"]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='id string, vec string')
        
        agnes = AgnesBatchOp()\
            .setIdCol("id")\
            .setVectorCol("vec")\
            .setPredictionCol("pred")\
            .linkFrom(inOp)
            
        agnes.print()
        pass