import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestPCA(unittest.TestCase):
    def test_pca(self):

        df = pd.DataFrame([
                [0.0,0.0,0.0],
                [0.1,0.2,0.1],
                [0.2,0.2,0.8],
                [9.0,9.5,9.7],
                [9.1,9.1,9.6],
                [9.2,9.3,9.9]
        ])
        
        # batch source 
        inOp = BatchOperator.fromDataframe(df, schemaStr='x1 double, x2 double, x3 double')
        
        pca = PCA().setK(2).setSelectedCols(["x1","x2","x3"]).setPredictionCol("pred")
        
        # train
        model = pca.fit(inOp)
        
        # batch predict
        model.transform(inOp).print()
        
        # stream predict
        inStreamOp = StreamOperator.fromDataframe(df, schemaStr='x1 double, x2 double, x3 double')
        
        model.transform(inStreamOp).print()
        
        StreamOperator.execute()
        pass