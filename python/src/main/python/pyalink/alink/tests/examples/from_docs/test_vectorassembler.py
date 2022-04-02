import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorAssembler(unittest.TestCase):
    def test_vectorassembler(self):

        df = pd.DataFrame([
            ["0", "$6$1:2.0 2:3.0 5:4.3", "3.0 2.0 3.0"],
            ["1", "$8$1:2.0 2:3.0 7:4.3", "3.0 2.0 3.0"],
            ["2", "$8$1:2.0 2:3.0 7:4.3", "2.0 3.0"]
        ])
        data = BatchOperator.fromDataframe(df, schemaStr="id string, c0 string, c1 string")
        
        res = VectorAssembler()\
        			.setSelectedCols(["c0", "c1"])\
        			.setOutputCol("table2vec")
        res.transform(data).print()
        pass