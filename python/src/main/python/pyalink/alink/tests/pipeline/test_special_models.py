import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestSpecialModels(unittest.TestCase):

    def test_wrap_model(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        stage1 = QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["sepal_length"])
        model1 = stage1.fit(source)
        output1 = model1.transform(source)
        print(model1)
        print(dir(model1))
        pass

    def test_glr(self):
        data = np.array([
            [1, 5, 118, 69, 1.0, 2.0],
            [2, 10, 58, 35, 1.0, 2.0],
            [3, 15, 42, 26, 1.0, 2.0],
            [4, 20, 35, 21, 1.0, 2.0],
            [5, 30, 27, 18, 1.0, 2.0],
            [6, 40, 25, 16, 1.0, 2.0],
            [7, 60, 21, 13, 1.0, 2.0],
            [8, 80, 19, 12, 1.0, 2.0],
            [9, 100, 18, 12, 1.0, 2.0]
        ])
        df = pd.DataFrame({
            "id": data[:, 0],
            "u": data[:, 1],
            "lot1": data[:, 2],
            "lot2": data[:, 3],
            "offset": data[:, 4],
            "weights": data[:, 5],
        }).astype({
            "id": np.int64,
            "u": np.int64,
            "lot1": np.int64,
            "lot2": np.int64,
            "offset": np.float64,
            "weights": np.float64,
        })
        print(df)
        print(df.dtypes)

        source = dataframeToOperator(df,
                                     schemaStr="id int, u double, lot1 double, lot2 double, offset double, weights double",
                                     op_type="batch")

        featureColNames = ["lot1", "lot2"]
        labelColName = "u"

        glm = GeneralizedLinearRegression() \
            .setFamily("gamma") \
            .setLink("Log") \
            .setRegParam(0.3) \
            .setFitIntercept(False) \
            .setMaxIter(10) \
            .setOffsetCol("offset") \
            .setWeightCol("weights") \
            .setFitIntercept(False) \
            .setFeatureCols(featureColNames) \
            .setLabelCol(labelColName) \
            .setPredictionCol("pred")

        model = glm.fit(source)
        print(type(model))
        print(dir(model))
        print(model.transform(source).collectToDataframe())
