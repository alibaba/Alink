import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestIForestModelOutlierPredictStreamOp(unittest.TestCase):
    def test_iforestmodeloutlierpredictstreamop(self):

        import pandas as pd
        df = pd.DataFrame([
        [0.73, 0],
        [0.24, 0],
        [0.63, 0],
        [0.55, 0],
        [0.73, 0],
        [0.41, 0]
        ])
        
        dataOp = BatchOperator.fromDataframe(df, schemaStr='val double, label int')
        
        streamDataOp = StreamOperator.fromDataframe(df, schemaStr='val double, label int')
        
        trainOp = IForestModelOutlierTrainBatchOp()\
        .setFeatureCols(["val"])
        
        predOp = IForestModelOutlierPredictStreamOp(trainOp.linkFrom(dataOp))\
        .setOutlierThreshold(3.0)\
        .setPredictionCol("pred")\
        .setPredictionDetailCol("pred_detail")
        
        predOp.linkFrom(streamDataOp).print()
        
        StreamOperator.execute()
        pass