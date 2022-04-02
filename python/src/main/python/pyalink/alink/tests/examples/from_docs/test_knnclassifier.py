import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestKnnClassifier(unittest.TestCase):
    def test_knnclassifier(self):

        df = pd.DataFrame([
          [1, "0,0,0"],
          [1, "0.1,0.1,0.1"],
          [1, "0.2,0.2,0.2"],
          [0, "9,9,9"],
          [0, "9.1,9.1,9.1"],
          [0, "9.2,9.2,9.2"]
        ])
        
        dataSource = BatchOperator.fromDataframe(df, schemaStr="label int, vec string")
        knn = KnnClassifier().setVectorCol("vec") \
            .setPredictionCol("pred") \
            .setLabelCol("label") \
            .setK(3)
        
        model = knn.fit(dataSource)
        model.transform(dataSource).print()
        pass