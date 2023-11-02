import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestOnePassClusterStreamOp(unittest.TestCase):
    def test_onepassclusterstreamop(self):

        df = pd.DataFrame([
          [0, "0 0 0"],
          [1, "0.1,0.1,0.1"],
          [2, "0.2,0.2,0.2"],
          [3, "9 9 9"],
          [4, "9.1 9.1 9.1"],
          [5, "9.2 9.2 9.2"]
        ])
        
        batch_data = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
        stream_data = StreamOperator.fromDataframe(df, schemaStr='id int, vec string')
        
        init_model = KMeansTrainBatchOp()\
            .setVectorCol("vec")\
            .setK(2)\
            .linkFrom(batch_data)
        
        onepassCluster = OnePassClusterStreamOp(init_model) \
          .setPredictionCol("pred")\
          .setPredictionDetailCol("distance")\
          .setModelOutputInterval(100)\
          .setEpsilon(1.)\
          .linkFrom(stream_data)
        onepassCluster.print()
        StreamOperator.execute()
        
        pass