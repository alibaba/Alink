import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestAppendModelStreamFileSinkBatchOp(unittest.TestCase):
    def test_appendmodelstreamfilesinkbatchop(self):

        df = pd.DataFrame([
            [1.0, "A", 0, 0, 0, 1.0],
            [2.0, "B", 1, 1, 0, 2.0],
            [3.0, "C", 2, 2, 1, 3.0],
            [4.0, "D", 3, 3, 1, 4.0]
        ])
        
        input = BatchOperator.fromDataframe(df, schemaStr='f0 double, f1 string, f2 int, f3 int, label int, reg_label double')
        
        rfOp = RandomForestTrainBatchOp()\
            .setLabelCol("reg_label")\
            .setFeatureCols(["f0", "f1", "f2", "f3"])\
            .setFeatureSubsamplingRatio(0.5)\
            .setSubsamplingRatio(1.0)\
            .setNumTreesOfInfoGain(1)\
            .setNumTreesOfInfoGain(1)\
            .setNumTreesOfInfoGainRatio(1)\
            .setCategoricalCols(["f1"])
        
        modelStream = AppendModelStreamFileSinkBatchOp()\
            .setFilePath("/tmp/random_forest_model_stream")\
            .setNumKeepModel(10)
        
        rfOp.linkFrom(input).link(modelStream)
        
        BatchOperator.execute()
        pass