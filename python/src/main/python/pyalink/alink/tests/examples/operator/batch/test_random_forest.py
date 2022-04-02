import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestPinqi(unittest.TestCase):

    def test_random_forest(self):
        data = np.array([
            [1, 2, 0.8],
            [1, 2, 0.7],
            [0, 3, 0.4],
            [0, 2, 0.4],
            [1, 3, 0.6],
            [4, 3, 0.2],
            [4, 4, 0.3]
        ])
        colNames = ['col0', 'col1', 'label']
        df = pd.DataFrame({"col0": data[:, 0],
                           "col1": data[:, 1],
                           "label": data[:, 2]})
        batchData = dataframeToOperator(df, schemaStr='col0 double, col1 double, label double', op_type='batch')
        batchData.print()
        decisionTreeTrainBatchOp = DecisionTreeTrainBatchOp()\
            .setLabelCol(colNames[2])\
            .setFeatureCols([colNames[0], colNames[1]])\
            .setMinSamplesPerLeaf(1)\
            .setMaxMemoryInMB(1)\
            .setCreateTreeMode("parallel") \
            .linkFrom(batchData)

        decisionTreeTrainBatchOp.print()

        def model_info_callback(d: DecisionTreeModelInfo):
            self.assertEqual(type(d), DecisionTreeModelInfo)
            print(d.getCaseWhenRule())

        decisionTreeTrainBatchOp.lazyCollectModelInfo(model_info_callback)
        model_info: DecisionTreeModelInfo = decisionTreeTrainBatchOp.collectModelInfo()
        print(model_info)
