import unittest
from pyalink.alink import *
import numpy as np
import pandas as pd
class TestKSigmaOutlier4GroupedDataBatchOp(unittest.TestCase):
    def test_ksigmaoutlier4groupeddatabatchop(self):

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
        
        dataOp = BatchOperator.fromDataframe(df, schemaStr='group_id int, id int, val double')
        
        
        outlierOp = dataOp.link(\
                        GroupByBatchOp()\
                            .setGroupByPredicate("group_id")\
                            .setSelectClause("mtable_agg(id, val) as data")\
        		).link(\
                    KSigmaOutlier4GroupedDataBatchOp()\
                        .setInputMTableCol("data")\
                        .setOutputMTableCol("pred")\
                        .setFeatureCol("val")\
                        .setPredictionCol("detect_pred")\
        		)
        
        outlierOp.print()
        pass