import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestIForestOutlier4GroupedData(unittest.TestCase):
    def test_iforestoutlier4groupeddata(self):

        import pandas as pd
        df = pd.DataFrame([
            [1, 1, 10.0],
            [1, 2, 11.0],
            [1, 3, 12.0],
            [1, 4, 13.0],
            [1, 5, 14.0],
            [1, 6, 15.0],
            [1, 7, 16.0],
            [1, 8, 17.0],
            [1, 9, 18.0],
            [1, 10, 19.0]
        ])
        
        dataOp = BatchOperator.fromDataframe(
            df, schemaStr='group_id int, id int, val double')
        
        IForestOutlier4GroupedData()\
            .setInputMTableCol("data")\
            .setOutputMTableCol("pred")\
            .setFeatureCols(["val"])\
            .setPredictionCol("detect_pred")\
            .transform(
            dataOp.link(
                GroupByBatchOp()
                .setGroupByPredicate("group_id")
                .setSelectClause("group_id, mtable_agg(id, val) as data")
            )
        ).print()
        pass