import tempfile
import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestModelInfo(unittest.TestCase):

    def test_get_model_info(self):
        data = np.array([
            ["a", 2, 1, 1],
            ["a", 3, 2, 1],
            ["a", 4, 3, 2],
            ["a", 2, 4, 1],
            ["a", 2, 2, 1],
            ["a", 4, 3, 2],
            ["a", 1, 2, 1],
            ["a", 5, 3, 3]])
        df = pd.DataFrame.from_records(data)
        batchData = dataframeToOperator(df, schemaStr='f2 string, f0 int, f1 int, label int', op_type='batch')
        colnames = ["f2", "f0", "f1"]
        lasso = CartRegTrainBatchOp()\
            .setFeatureCols(colnames)\
            .setLabelCol("label") \
            .lazyPrintModelInfo()\
            .linkFrom(batchData)

        predictor = CartRegPredictBatchOp().setPredictionCol("pred")
        predictor.linkFrom(lasso, batchData).print()

    def test_extract_model_info(self):
        model_filename = tempfile.NamedTemporaryFile().name
        print(model_filename)
        data = np.array([
            ["a", 2, 1, 1],
            ["a", 3, 2, 1],
            ["a", 4, 3, 2],
            ["a", 2, 4, 1],
            ["a", 2, 2, 1],
            ["a", 4, 3, 2],
            ["a", 1, 2, 1],
            ["a", 5, 3, 3]])
        df = pd.DataFrame.from_records(data)
        batchData = dataframeToOperator(df, schemaStr='f2 string, f0 int, f1 int, label int', op_type='batch')
        colnames = ["f2", "f0", "f1"]
        lasso = CartRegTrainBatchOp()\
            .setFeatureCols(colnames)\
            .setLabelCol("label")\
            .linkFrom(batchData)
        lasso.link(
            AkSinkBatchOp().setFilePath(model_filename)
        )
        BatchOperator.execute()

        model_source = AkSourceBatchOp().setFilePath(model_filename)
        model_source.link(
            DecisionTreeModelInfoBatchOp().lazyPrintModelInfo()
        )
        BatchOperator.execute()
