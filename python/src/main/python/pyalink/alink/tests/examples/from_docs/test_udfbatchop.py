import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestUDFBatchOp(unittest.TestCase):
    def test_udfbatchop(self):

        # 4种 UDF 定义
        
        # ScalarFunction
        class PlusOne(ScalarFunction):
            def eval(self, x, y):
                return x + y + 10
        f_udf1 = udf(PlusOne(), input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_type=DataTypes.DOUBLE())
        
        # function + decorator
        @udf(input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_type=DataTypes.DOUBLE())
        def f_udf2(x, y):
            return x + y + 20
        
        # function
        def f_udf3(x, y):
            return x + y + 30
        f_udf3 = udf(f_udf3, input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_type=DataTypes.DOUBLE())
        
        # lambda function
        f_udf4 = udf(lambda x, y: x + y + 40
                       , input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_type=DataTypes.DOUBLE())
        
        udfs = [
            f_udf1,
            f_udf2,
            f_udf3,
            f_udf4
        ]
        
        # 4种 UDTF 定义
        
        # TableFunction
        class SplitOp(TableFunction):
            def eval(self, *args):
                for index, arg in enumerate(args):
                    yield index, arg
        f_udtf1 = udtf(SplitOp(), [DataTypes.DOUBLE(), DataTypes.DOUBLE()], [DataTypes.INT(), DataTypes.DOUBLE()])
        
        # function + decorator
        @udtf(input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_types=[DataTypes.INT(), DataTypes.DOUBLE()])
        def f_udtf2(*args):
            for index, arg in enumerate(args):
                yield index, arg
        
        # function
        def f_udtf3(*args):
            for index, arg in enumerate(args):
                yield index, arg
        f_udtf3 = udtf(f_udtf3, input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_types=[DataTypes.INT(), DataTypes.DOUBLE()])
        
        # lambda function
        f_udtf4 = udtf(lambda *args: [ (yield index, arg) for index, arg in enumerate(args) ]
                       , input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_types=[DataTypes.INT(), DataTypes.DOUBLE()])
        
        udtfs = [
            f_udtf1,
            f_udtf2,
            f_udtf3,
            f_udtf4
        ]
        pass