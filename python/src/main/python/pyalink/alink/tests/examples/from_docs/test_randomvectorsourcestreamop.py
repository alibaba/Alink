import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestRandomVectorSourceStreamOp(unittest.TestCase):
    def test_randomvectorsourcestreamop(self):

        RandomVectorSourceStreamOp().setMaxRows(5).setSize([2]).setSparsity(1.0).print()
        StreamOperator.execute()
        pass