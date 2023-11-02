import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestIndexToStringPredictBatchOp(unittest.TestCase):
    def test_indextostringpredictbatchop(self):

        df_data = pd.DataFrame([
            ["football"],
            ["football"],
            ["football"],
            ["basketball"],
            ["basketball"],
            ["tennis"],
        ])
        
        train_data = BatchOperator.fromDataframe(df_data, schemaStr='f0 string')
        
        stringIndexer = StringIndexer()\
            .setModelName("string_indexer_model")\
            .setSelectedCol("f0")\
            .setOutputCol("f0_indexed")\
            .setStringOrderType("frequency_asc").fit(train_data)
        
        indexed = stringIndexer.transform(train_data)
        
        indexToStrings = IndexToStringPredictBatchOp()\
            .setSelectedCol("f0_indexed")\
            .setOutputCol("f0_indxed_unindexed")
        
        indexToStrings.linkFrom(stringIndexer.getModelData(), indexed).print()
        pass