import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestBertTextEmbeddingStreamOp(unittest.TestCase):
    def test_berttextembeddingstreamop(self):

        df_data = pd.DataFrame([
            [1, 'An english sentence.'],
            [2, '这是一个中文句子']
        ])
        
        stream_data = StreamOperator.fromDataframe(df_data, schemaStr='f1 bigint, f2 string')
        
        BertTextEmbeddingStreamOp() \
            .setSelectedCol("f2") \
            .setOutputCol("embedding") \
            .setLayer(-2) \
            .linkFrom(stream_data) \
            .print()
        
        StreamOperator.execute()
        pass