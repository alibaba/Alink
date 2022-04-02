import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestGmm(unittest.TestCase):

    def test_gmm(self):
        data = np.array([
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

        df_data = pd.DataFrame({
            "features": data[:, 0],
        })

        data = dataframeToOperator(df_data, schemaStr='features string', op_type="batch")

        gmm = GmmTrainBatchOp() \
            .setVectorCol("features") \
            .setMaxIter(10)

        model = gmm.linkFrom(data)

        from pyalink.alink.common.types.model_info import GmmModelInfo
        from pyalink.alink.common.types.vector import DenseMatrix

        def model_info_callback(d: GmmModelInfo):
            self.assertEquals(type(d), GmmModelInfo)
            print(d)
            print("Cluster Number", d.getClusterNumber())
            print("Cluster center", d.getClusterCenter(1))
            print("Cluster covar matrix", d.getClusterCovarianceMatrix(0))
            cov_mat: DenseMatrix = d.getClusterCovarianceMatrix(0)
            print(cov_mat.get(0, 0))
        model.lazyCollectModelInfo(model_info_callback)

        predictor = GmmPredictBatchOp() \
            .setPredictionCol("cluster_id") \
            .setVectorCol("features") \
            .setPredictionDetailCol("cluster_detail")

        predictor.linkFrom(model, data).print()
