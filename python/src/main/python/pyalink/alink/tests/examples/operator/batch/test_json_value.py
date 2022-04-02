import unittest

from pyalink.alink import *


class TestYuhe(unittest.TestCase):

    def test_json_value_op(self):
        import numpy as np
        import pandas as pd
        data = np.array([
            ["{a:boy,b:{b1:1,b2:2}}"],
            ["{a:girl,b:{b1:1,b2:2}}"]])
        df = pd.DataFrame({"str": data[:, 0]})

        batchData = dataframeToOperator(df, schemaStr='str string', op_type='batch')

        JsonValueBatchOp().setJsonPath(["$.a", "$.b.b1"]).setSelectedCol("str").setOutputCols(["f0", "f1"]).linkFrom(
            batchData).print()
