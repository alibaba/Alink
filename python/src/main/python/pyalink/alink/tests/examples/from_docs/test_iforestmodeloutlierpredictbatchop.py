import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestIForestModelOutlierPredictBatchOp(unittest.TestCase):
    def test_iforestmodeloutlierpredictbatchop(self):

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
        
        trainOp = IForestModelOutlierTrainBatchOp()\
        .setFeatureCols(["val"])
        
        predOp = IForestModelOutlierPredictBatchOp()\
        .setOutlierThreshold(3.0)\
        .setPredictionCol("pred")\
        .setPredictionDetailCol("pred_detail")
        
        predOp.linkFrom(trainOp.linkFrom(dataOp), dataOp)
        
        evalOp = EvalOutlierBatchOp()\
        .setLabelCol("label")\
        .setPredictionDetailCol("pred_detail")\
        .setOutlierValueStrings(["1"]);
        
        metrics = predOp\
        .link(evalOp)\
        .collectMetrics()
        
        print(metrics)
        pass