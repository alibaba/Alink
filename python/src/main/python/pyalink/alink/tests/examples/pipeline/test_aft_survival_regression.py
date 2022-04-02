import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestQingzhao(unittest.TestCase):

    def test_aft_survival_regression(self):
        data = np.array([[1.218, 1.0, "1.560,-0.605"], \
                         [2.949, 0.0, "0.346,2.158"], \
                         [3.627, 0.0, "1.380,0.231"], \
                         [0.273, 1.0, "0.520,1.151"], \
                         [4.199, 0.0, "0.795,-0.226"]])
        df = pd.DataFrame({"label": data[:, 0], "censor": data[:, 1], "features": data[:, 2]})
        data = dataframeToOperator(df, schemaStr="label double, censor double, features string", op_type="batch")

        reg = AftSurvivalRegression() \
            .setVectorCol("features") \
            .setLabelCol("label") \
            .setCensorCol("censor") \
            .setPredictionCol("result")
        pipeline = Pipeline().add(reg)
        model = pipeline.fit(data)
        model.save().collectToDataframe()
        model.transform(data).collectToDataframe()
