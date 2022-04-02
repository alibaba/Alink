import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestQingzhao(unittest.TestCase):

    def test_vector_assembler(self):
        data = np.array([["0", "$6$1:2.0 2:3.0 5:4.3", "3.0 2.0 3.0"], \
                         ["1", "$8$1:2.0 2:3.0 7:4.3", "3.0 2.0 3.0"], \
                         ["2", "$8$1:2.0 2:3.0 7:4.3", "2.0 3.0"]])
        df = pd.DataFrame({"id": data[:, 0], "c0": data[:, 1], "c1": data[:, 2]})
        data = dataframeToOperator(df, schemaStr="id string, c0 string, c1 string", op_type="batch")

        res = VectorAssembler() \
            .setSelectedCols(["c0", "c1"]) \
            .setOutputCol("table2vec")
        res.transform(data).print()
