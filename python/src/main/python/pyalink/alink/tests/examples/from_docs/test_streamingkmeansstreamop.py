import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestStreamingKMeansStreamOp(unittest.TestCase):
    def test_streamingkmeansstreamop(self):

        df = pd.DataFrame([
          [0, "0 0 0"],
          [1, "0.1,0.1,0.1"],
          [2, "0.2,0.2,0.2"],
          [3, "9 9 9"],
          [4, "9.1 9.1 9.1"],
          [5, "9.2 9.2 9.2"]
        ])
        
        inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
        stream_data = StreamOperator.fromDataframe(df, schemaStr='id int, vec string')
        
        init_model = KMeansTrainBatchOp()\
            .setVectorCol("vec")\
            .setK(2)\
            .linkFrom(inOp)
        
        streamingkmeans = StreamingKMeansStreamOp(init_model) \
          .setTimeInterval(1) \
          .setHalfLife(1) \
          .setReservedCols(["vec"])
        
        pred = streamingkmeans.linkFrom(stream_data, stream_data)
        
        pred.print()
        StreamOperator.execute()
        
        pass