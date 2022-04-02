import unittest

from pyalink.alink import *


class TestDataTypes(unittest.TestCase):

    def test_types(self):
        types = [
            # pyflink does not have: "VOID", "CHAR", "BIGINTEGER"
            (DataTypes.BOOLEAN(), ("BOOLEAN", "BOOL")),
            (DataTypes.TINYINT(), "BYTE"),
            (DataTypes.SMALLINT(), "SHORT"),
            (DataTypes.INT(), ("INT", "INTEGER")),
            (DataTypes.BIGINT(), ("BIGINT", "LONG")),
            (DataTypes.FLOAT(), "FLOAT"),
            (DataTypes.DOUBLE(), "DOUBLE"),
            (DataTypes.DECIMAL(38, 18), "BIGDECIMAL"),

            (DataTypes.DATE(), "DATE"),
            (DataTypes.TIME(), "TIME"),
            (DataTypes.TIMESTAMP(precision=3), ("TIMESTAMP", "DATETIME")),

            (DataTypes.STRING(), "STRING"),
            # alink java does not have:
            (DataTypes.VARCHAR(2147483647), "STRING"),
            # CharType not supported by pyflink
            # (DataTypes.CHAR(1), "STRING"),
        ]

        from pyalink.alink.udf.utils import _to_flink_type_string
        for t, w in types:
            self.assertIn(_to_flink_type_string(t), w)
        pass
