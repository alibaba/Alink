import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestOcsvmModelOutlierPredictBatchOp(unittest.TestCase):
    def test_ocsvmmodeloutlierpredictbatchop(self):

        data = RandomTableSourceBatchOp()\
            .setNumCols(5)\
            .setNumRows(1000)\
            .setIdCol("id")\
            .setOutputCols(["x1", "x2", "x3", "x4"])
        
        dataTest = data
        ocsvm = OcsvmModelOutlierTrainBatchOp().setFeatureCols(["x1", "x2", "x3", "x4"]).setGamma(0.5).setNu(0.1).setKernelType("RBF")
        model = data.link(ocsvm)
        predictor = OcsvmModelOutlierPredictBatchOp().setPredictionCol("pred")
        predictor.linkFrom(model, dataTest).print()
        pass