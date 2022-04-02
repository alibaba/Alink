import unittest

import numpy as np
import pandas as pd

from pyalink.alink.common.types import NaiveBayesTextModelInfo
from pyalink.alink import *


class TestYuhe(unittest.TestCase):

    def test_naive_bayes_op(self):
        data = np.array([
            ["$31$0:1.0 1:1.0 2:1.0 30:1.0", "1.0  1.0  1.0  1.0", '1'],
            ["$31$0:1.0 1:1.0 2:0.0 30:1.0", "1.0  1.0  0.0  1.0", '1'],
            ["$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", '1'],
            ["$31$0:1.0 1:0.0 2:1.0 30:1.0", "1.0  0.0  1.0  1.0", '1'],
            ["$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", '0'],
            ["$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", '0'],
            ["$31$0:0.0 1:1.0 2:1.0 30:0.0", "0.0  1.0  1.0  0.0", '0']])

        dataSchema = ["sv", "dv", "label"]

        df = pd.DataFrame({"sv": data[:, 0], "dv": data[:, 1], "label": data[:, 2]})
        batchData = dataframeToOperator(df, schemaStr='sv string, dv string, label string', op_type='batch')

        ns = NaiveBayesTextTrainBatchOp().setVectorCol("sv").setLabelCol("label")
        model = batchData.link(ns)

        def model_info_callback(d: NaiveBayesTextModelInfo):
            self.assertEquals(type(d), NaiveBayesTextModelInfo)
            print("1:", d.getModelType())
            print("2:", d.getPriorProbability())
            print("3:", d.getFeatureProbability())
            self.assertEquals(type(d.getPriorProbability()), list)
            self.assertEquals(type(d.getFeatureProbability()), DenseMatrix)
            print("4:", d.getVectorColName())

        model.lazyCollectModelInfo(model_info_callback)

        predictor = NaiveBayesTextPredictBatchOp().setVectorCol("sv").setReservedCols(["sv", "label"]).setPredictionCol(
            "pred")
        predictor.linkFrom(model, batchData).print()
