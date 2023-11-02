import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestSummarizerBatchOp(unittest.TestCase):
    def test_summarizerbatchop(self):

        df = pd.DataFrame([
            ["a", 1, 1,2.0, True],
            ["c", 1, 2, -3.0, True],
            ["a", 2, 2,2.0, False],
            ["c", 0, 0, 0.0, False]
        ])
        source = BatchOperator.fromDataframe(df, schemaStr='f_string string, f_long long, f_int int, f_double double, f_boolean boolean')
        
        summarizer = SummarizerBatchOp()\
            .setSelectedCols(["f_long", "f_int", "f_double"])
        
        summary = summarizer.linkFrom(source).collectSummary()
        
        print(summary)
        pass