import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestSomBatchOp(unittest.TestCase):
    def test_sombatchop(self):

        df = pd.DataFrame([
            [5,2,3.5,1,'Iris-versicolor'],
            [5.1,3.7,1.5,0.4,'Iris-setosa'],
            [6.4,2.8,5.6,2.2,'Iris-virginica'],
            [6,2.9,4.5,1.5,'Iris-versicolor'],
            [4.9,3,1.4,0.2,'Iris-setosa'],
            [5.7,2.6,3.5,1,'Iris-versicolor'],
            [4.6,3.6,1,0.2,'Iris-setosa'],
            [5.9,3,4.2,1.5,'Iris-versicolor'],
            [6.3,2.8,5.1,1.5,'Iris-virginica'],
            [4.7,3.2,1.3,0.2,'Iris-setosa'],
            [5.1,3.3,1.7,0.5,'Iris-setosa'],
            [5.5,2.4,3.8,1.1,'Iris-versicolor'],
        ])
        
        source = BatchOperator.fromDataframe(df, schemaStr='sepal_length double, sepal_width double, petal_length double, petal_width double, category string')
        
        va = VectorAssemblerBatchOp().setSelectedCols(["sepal_length", "sepal_width"]) \
          .setOutputCol("features")
        
        som = SomBatchOp()\
          .setXdim(2) \
          .setYdim(4) \
          .setVdim(2) \
          .setSigma(1.0) \
          .setNumIters(10) \
          .setVectorCol("features")
        
        source.link(va).link(som).print()
        
        pass