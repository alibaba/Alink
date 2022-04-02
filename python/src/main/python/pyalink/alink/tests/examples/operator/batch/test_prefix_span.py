import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPrefixSpan(unittest.TestCase):

    def test_prefix_span(self):
        data = np.array([
            ["a;a,b,c;a,c;d;c,f"],
            ["a,d;c;b,c;a,e"],
            ["e,f;a,b;d,f;c;b"],
            ["e;g;a,f;c;b;c"],
        ])

        df_data = pd.DataFrame({
            "sequence": data[:, 0],
        })

        data = dataframeToOperator(df_data, schemaStr='sequence string', op_type="batch")

        prefixSpan = PrefixSpanBatchOp() \
            .setItemsCol("sequence") \
            .setMinSupportCount(3)

        prefixSpan.linkFrom(data)

        prefixSpan.print()
        prefixSpan.getSideOutput(0).print()
