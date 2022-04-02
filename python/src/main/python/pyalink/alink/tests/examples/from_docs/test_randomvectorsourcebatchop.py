import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestRandomVectorSourceBatchOp(unittest.TestCase):
    def test_randomvectorsourcebatchop(self):

        RandomVectorSourceBatchOp().setNumRows(5).setSize([2]).setSparsity(1.0).print()
        pass