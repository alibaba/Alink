import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestMultilayerPerceptronPredictBatchOp(unittest.TestCase):
    def test_multilayerperceptronpredictbatchop(self):

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
        
        data = BatchOperator.fromDataframe(df, schemaStr='sepal_length double, sepal_width double, petal_length double, petal_width double, category string')
        
        mlpc = MultilayerPerceptronTrainBatchOp() \
          .setFeatureCols(["sepal_length", "sepal_width", "petal_length", "petal_width"]) \
          .setLabelCol("category") \
          .setLayers([4, 8, 3]) \
          .setMaxIter(10)
        
        model = mlpc.linkFrom(data)
        
        predictor = MultilayerPerceptronPredictBatchOp()\
          .setPredictionCol('p')
        
        predictor.linkFrom(model, data).print()
        
        pass