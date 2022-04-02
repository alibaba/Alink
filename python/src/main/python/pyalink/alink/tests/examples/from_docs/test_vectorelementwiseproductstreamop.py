import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestVectorElementwiseProductStreamOp(unittest.TestCase):
    def test_vectorelementwiseproductstreamop(self):

        df = pd.DataFrame([
            ["1:3,2:4,4:7", 1],
            ["0:3,5:5", 3],
            ["2:4,4:5", 4]
        ])
        
        data = StreamOperator.fromDataframe(df, schemaStr="vec string, id bigint")
        
        vecEP = VectorElementwiseProductStreamOp().setSelectedCol("vec") \
        	.setOutputCol("vec1") \
        	.setScalingVector("$8$1:3.0 3:3.0 5:4.6")
        data.link(vecEP).print()
        StreamOperator.execute()
        pass