import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestBertTextEmbeddingBatchOp(unittest.TestCase):
    def test_berttextembeddingbatchop(self):

        df_data = pd.DataFrame([
            [1, 'An english sentence.'],
            [2, '这是一个中文句子']
        ])
        
        batch_data = BatchOperator.fromDataframe(df_data, schemaStr='f1 bigint, f2 string')
        
        BertTextEmbeddingBatchOp() \
            .setSelectedCol("f2") \
            .setOutputCol("embedding") \
            .setLayer(-2) \
            .linkFrom(batch_data) \
            .print()
        pass