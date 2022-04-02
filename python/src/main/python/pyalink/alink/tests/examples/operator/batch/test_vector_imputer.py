import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestQingzhao(unittest.TestCase):

    def run_vector_imputer_op(self):
        data = np.array([["1:3,2:4,4:7", 1], \
                         ["1:3,2:NaN", 3], \
                         ["2:4,4:5", 4]])
        df = pd.DataFrame({"vec": data[:, 0], "id": data[:, 1]})
        data = dataframeToOperator(df, schemaStr="vec string, id bigint", op_type="batch")
        vecFill = VectorImputerTrainBatchOp().setSelectedCol("vec")
        model = data.link(vecFill)
        VectorImputerPredictBatchOp().setOutputCol("vec1").linkFrom(model, data).print()
