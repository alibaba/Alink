import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPinyi(unittest.TestCase):

    def test_n_gram_op(self):
        data = np.array([
            [0, 'That is an English Book!'],
            [1, 'Do you like math?'],
            [2, 'Have a good day!']
        ])

        df = pd.DataFrame({"id": data[:, 0], "text": data[:, 1]})
        inOp1 = dataframeToOperator(df, schemaStr='id long, text string', op_type='batch')

        op = NGramBatchOp().setSelectedCol("text")
        print(BatchOperator.collectToDataframe(op.linkFrom(inOp1)))
