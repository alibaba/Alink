import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorImputerPredictBatchOp(unittest.TestCase):
    def test_vectorimputerpredictbatchop(self):

        df = pd.DataFrame([
            ["1:3,2:4,4:7", 1],
            ["1:3,2:NaN", 3],
            ["2:4,4:5", 4]
        ])
        
        data = BatchOperator.fromDataframe(df, schemaStr="vec string, id bigint")
        vecFill = VectorImputerTrainBatchOp().setSelectedCol("vec")
        model = data.link(vecFill)
        VectorImputerPredictBatchOp().setOutputCol("vec1").linkFrom(model, data).print()
        pass