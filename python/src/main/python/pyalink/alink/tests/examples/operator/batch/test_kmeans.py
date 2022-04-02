import unittest

from pyalink.alink import *


class TestPinjiu(unittest.TestCase):

    def test_kmeans_op(self):
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
        inOp1 = BatchOperator.fromDataframe(df, schemaStr='id int, vec string')
        inOp2 = StreamOperator.fromDataframe(df, schemaStr='id int, vec string')
        kmeans = KMeansTrainBatchOp().setVectorCol("vec").setK(2)
        predictBatch = KMeansPredictBatchOp().setPredictionCol("pred")
        kmeans.linkFrom(inOp1)

        from pyalink.alink.common.types.model_info import KMeansModelInfo

        def kmeans_model_info_callback(d: KMeansModelInfo):
            self.assertEquals(type(d), KMeansModelInfo)

        kmeans.lazyCollectModelInfo(kmeans_model_info_callback)

        kmeans_model_info_batch_op = KMeansModelInfoBatchOp() \
            .linkFrom(kmeans)
        kmeans_model_info_batch_op.lazyCollectModelInfo(kmeans_model_info_callback)

        predictBatch.linkFrom(kmeans, inOp1)
        [model, predict] = collectToDataframes(kmeans, predictBatch)
        print(model)
        print(predict)
