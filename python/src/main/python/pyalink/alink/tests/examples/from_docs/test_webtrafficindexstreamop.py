import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestWebTrafficIndexStreamOp(unittest.TestCase):
    def test_webtrafficindexstreamop(self):

        data = RandomTableSourceStreamOp()\
                .setNumCols(2)\
                .setMaxRows(50)\
                .setIdCol("id")\
                .setOutputCols(["f0", "f1"]) \
                .setOutputColConfs("f0:uniform_open(1,2);f1:uniform(1,2)")\
                .setTimePerSample(0.1)
        
        op = WebTrafficIndexStreamOp()\
                .setTimeInterval(1)\
                .setSelectedCol("f0")\
                .linkFrom(data)
        
        op.print()
        
        StreamOperator.execute()
        pass