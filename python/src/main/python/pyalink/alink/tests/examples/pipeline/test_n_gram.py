import unittest

from pyalink.alink import *


class TestPinjiu(unittest.TestCase):

    def test_n_gram(self):
        import numpy as np
        import pandas as pd

        data = np.array([
            [0, 'That is an English Book!'],
            [1, 'Do you like math?'],
            [2, 'Have a good day!']
        ])

        df = pd.DataFrame({"id": data[:, 0], "text": data[:, 1]})
        inOp1 = dataframeToOperator(df, schemaStr='id long, text string', op_type='batch')

        op = NGram().setSelectedCol("text")
        op.transform(inOp1).print()
