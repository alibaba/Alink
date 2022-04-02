import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestQingzhao(unittest.TestCase):

    def run_vector_element_wise_product(self):
        data = [
            ["1:3,2:4,4:7", 1],
            ["0:3,5:5", 3],
            ["2:4,4:5", 4]]

        # load data
        data = np.array([["1:3,2:4,4:7", 1], \
                         ["0:3,5:5", 3], \
                         ["2:4,4:5", 4]])
        df = pd.DataFrame({"vec": data[:, 0], "id": data[:, 1]})
        data = dataframeToOperator(df, schemaStr="vec string, id bigint", op_type="batch")
        vecEP = VectorElementwiseProduct().setSelectedCol("vec") \
            .setOutputCol("vec1") \
            .setScalingVector("$8$1:3.0 3:3.0 5:4.6")
        vecEP.transform(data).print()
