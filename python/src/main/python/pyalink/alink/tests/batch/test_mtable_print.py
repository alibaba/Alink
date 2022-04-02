import unittest

from pyalink.alink import *


class TestMTablePrint(unittest.TestCase):

    def test_print_mtable(self):
        import datetime
        import pandas as pd

        data = pd.DataFrame([
            [1, datetime.datetime.fromtimestamp(1), 10.0],
            [1, datetime.datetime.fromtimestamp(2), 11.0],
            [1, datetime.datetime.fromtimestamp(3), 12.0],
            [1, datetime.datetime.fromtimestamp(4), 13.0],
            [1, datetime.datetime.fromtimestamp(5), 14.0],
            [1, datetime.datetime.fromtimestamp(6), 15.0],
            [1, datetime.datetime.fromtimestamp(7), 16.0],
            [1, datetime.datetime.fromtimestamp(8), 17.0],
            [1, datetime.datetime.fromtimestamp(9), 18.0],
            [1, datetime.datetime.fromtimestamp(10), 19.0]
        ])

        source = dataframeToOperator(data, schemaStr='id int, ts timestamp, val double', op_type='batch')

        res = GroupByBatchOp() \
            .setGroupByPredicate("id") \
            .setSelectClause("mtable_agg(ts, val) as data") \
            .linkFrom(source)
        print(res.getSchemaStr())
        res.print()
