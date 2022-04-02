import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestQingzhao(unittest.TestCase):

    def run_vector_interaction(self):
        data = np.array([["$8$1:3,2:4,4:7", "$8$1:3,2:4,4:7"], \
                         ["$8$0:3,5:5", "$8$1:2,2:4,4:7"], \
                         ["$8$2:4,4:5", "$8$1:3,2:3,4:7"]])
        df = pd.DataFrame({"vec": data[:, 0], "id": data[:, 1]})
        data = dataframeToOperator(df, schemaStr="vec1 string, vec2 string", op_type="batch")
        vecInter = VectorInteraction().setSelectedCols(["vec1", "vec2"]).setOutputCol("vec_product")
        vecInter.transform(data).print()
