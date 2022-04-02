import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestGmmPredictBatchOp(unittest.TestCase):
    def test_gmmpredictbatchop(self):

        df_data = pd.DataFrame([
            ["-0.6264538 0.1836433"],
            ["-0.8356286 1.5952808"],
            ["0.3295078 -0.8204684"],
            ["0.4874291 0.7383247"],
            ["0.5757814 -0.3053884"],
            ["1.5117812 0.3898432"],
            ["-0.6212406 -2.2146999"],
            ["11.1249309 9.9550664"],
            ["9.9838097 10.9438362"],
            ["10.8212212 10.5939013"],
            ["10.9189774 10.7821363"],
            ["10.0745650 8.0106483"],
            ["10.6198257 9.9438713"],
            ["9.8442045 8.5292476"],
            ["9.5218499 10.4179416"],
        ])
        
        data = BatchOperator.fromDataframe(df_data, schemaStr='features string')
        dataStream = StreamOperator.fromDataframe(df_data, schemaStr='features string')
        
        gmm = GmmTrainBatchOp() \
            .setVectorCol("features") \
            .setEpsilon(0.)
        
        model = gmm.linkFrom(data)
        
        predictor = GmmPredictBatchOp() \
            .setPredictionCol("cluster_id") \
            .setVectorCol("features") \
            .setPredictionDetailCol("cluster_detail")
        
        predictor.linkFrom(model, data).print()
        
        predictorStream = GmmPredictStreamOp(model) \
            .setPredictionCol("cluster_id") \
            .setVectorCol("features") \
            .setPredictionDetailCol("cluster_detail")
        
        predictorStream.linkFrom(dataStream).print()
        
        StreamOperator.execute()
        
        pass