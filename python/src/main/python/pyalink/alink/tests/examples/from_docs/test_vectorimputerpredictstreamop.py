import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorImputerPredictStreamOp(unittest.TestCase):
    def test_vectorimputerpredictstreamop(self):

        df = pd.DataFrame([
            ["1:3 2:4 4:7", 1],
            ["0:3 5:5", 3],
            ["2:4 4:5", 4]
        ])
        
        dataStream = StreamOperator.fromDataframe(df, schemaStr="vec string, id bigint")
        data = BatchOperator.fromDataframe(df, schemaStr="vec string, id bigint")
        
        vecFill = VectorImputerTrainBatchOp().setSelectedCol("vec")
        model = data.link(vecFill)
        VectorImputerPredictStreamOp(model).setOutputCol("vec1").linkFrom(dataStream).print()
        StreamOperator.execute()
        pass