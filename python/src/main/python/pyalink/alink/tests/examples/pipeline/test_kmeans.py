import unittest

from pyalink.alink import *


class TestPinjiu(unittest.TestCase):

    def test_kmeans(self):
        import numpy as np
        import pandas as pd
        data = np.array([
            [0, "0 0 0"],
            [1, "0.1,0.1,0.1"],
            [2, "0.2,0.2,0.2"],
            [3, "9 9 9"],
            [4, "9.1 9.1 9.1"],
            [5, "9.2 9.2 9.2"]
        ])
        df = pd.DataFrame({"id": data[:, 0], "vec": data[:, 1]})
        inOp = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
        kmeans = KMeans().setVectorCol("vec").setK(2).setPredictionCol("pred")
        kmeans.fit(inOp).transform(inOp).collectToDataframe()
