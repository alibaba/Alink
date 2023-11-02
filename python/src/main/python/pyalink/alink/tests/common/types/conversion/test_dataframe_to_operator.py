import unittest

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestDataFrame(unittest.TestCase):

    def setUp(self):
        data_null = np.array([
            ["007", 1, 1, 2.0, True],
            [None, 2, 2, None, True],
            ["12", None, 4, 2.0, False],
            ["1312", 0, None, 1.2, None],
        ])
        self.df_null = pd.DataFrame({
            "f_string": data_null[:, 0],
            "f_long": data_null[:, 1],
            "f_int": data_null[:, 2],
            "f_double": data_null[:, 3],
            "f_boolean": data_null[:, 4]
        })

        data = np.array([
            ["a", 1, 1, 2.0, True],
            ["abc", 2, 2, 2.4, True],
            ["c", 4, 4, 2.0, False],
            ["a", 0, 1, 1.2, False],
        ])
        self.df = pd.DataFrame({
            "f_string": data[:, 0],
            "f_long": data[:, 1],
            "f_int": data[:, 2],
            "f_double": data[:, 3],
            "f_boolean": data[:, 4]
        })

    def test_memory_null(self):
        from pyalink.alink.config import g_config
        g_config["collect_storage_type"] = "memory"
        schema = "f_string string,f_long long,f_int int,f_double double,f_boolean boolean"
        op = dataframeToOperator(self.df_null, schema, op_type="batch")

        col_names = op.getColNames()
        col_types = op.getColTypes()
        self.assertEqual(col_names[0], "f_string")
        self.assertEqual(col_names[1], "f_long")
        self.assertEqual(col_names[2], "f_int")
        self.assertEqual(col_names[3], "f_double")
        self.assertEqual(col_names[4], "f_boolean")

        self.assertEqual(col_types[0], "VARCHAR")
        self.assertEqual(col_types[1], "BIGINT")
        self.assertEqual(col_types[2], "INT")
        self.assertEqual(col_types[3], "DOUBLE")
        self.assertEqual(col_types[4], "BOOLEAN")

        df2 = op.collectToDataframe()
        print(df2)
        print(df2.dtypes)
        self.assertEqual(df2['f_string'].dtype, pd.StringDtype())
        self.assertEqual(df2['f_long'].dtype, pd.Int64Dtype())
        self.assertEqual(df2['f_int'].dtype, pd.Int32Dtype())
        self.assertEqual(df2['f_double'].dtype, np.float64)
        self.assertEqual(df2['f_boolean'].dtype, pd.BooleanDtype())

    def test_memory(self):
        from pyalink.alink.config import g_config
        g_config["collect_storage_type"] = "memory"
        schema = "f_string string,f_long long,f_int int,f_double double,f_boolean boolean"
        op = dataframeToOperator(self.df, schemaStr=schema, op_type="batch")

        col_names = op.getColNames()
        col_types = op.getColTypes()
        self.assertEqual(col_names[0], "f_string")
        self.assertEqual(col_names[1], "f_long")
        self.assertEqual(col_names[2], "f_int")
        self.assertEqual(col_names[3], "f_double")
        self.assertEqual(col_names[4], "f_boolean")

        self.assertEqual(col_types[0], "VARCHAR")
        self.assertEqual(col_types[1], "BIGINT")
        self.assertEqual(col_types[2], "INT")
        self.assertEqual(col_types[3], "DOUBLE")
        self.assertEqual(col_types[4], "BOOLEAN")

        df2 = op.collectToDataframe()
        print(df2)
        print(df2.dtypes)
        self.assertEqual(df2['f_string'].dtype, pd.StringDtype())
        self.assertEqual(df2['f_long'].dtype, pd.Int64Dtype())
        self.assertEqual(df2['f_int'].dtype, pd.Int32Dtype())
        self.assertEqual(df2['f_double'].dtype, np.float64)
        self.assertEqual(df2['f_boolean'].dtype, pd.BooleanDtype())

    def test_string_not_converted_to_double(self):
        data = np.array([
            ["007"],
            ["012"],
        ])
        source = dataframeToOperator(pd.DataFrame.from_records(data), schemaStr="str string", op_type="batch")
        df = source.collectToDataframe()
        print(df)
        self.assertEqual(df['str'].iloc[0], "007")
        self.assertEqual(df['str'].iloc[1], "012")

    def test_df_to_op_speed(self):
        import time
        start_time = time.time()

        m = {0: True, 1: False, 2: None}

        users = []
        for col in range(10000):
            r = col % 3
            users.append([col, "1\"" + str(col) + "\"1", m.get(r)])

        df = pd.DataFrame(users)
        source = BatchOperator.fromDataframe(df, schemaStr='id int, label string, b boolean')
        source.firstN(10).print()

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(elapsed_time)
        self.assertTrue(elapsed_time < 15)

    def test_op_to_df_speed(self):
        import time
        start_time = time.time()

        m = {0: True, 1: False, 2: None}

        users = []
        for col in range(50000):
            r = col % 3
            users.append([col, "1\"" + str(col) + "\"1", m.get(r)])

        df = pd.DataFrame(users)
        source = BatchOperator.fromDataframe(df, schemaStr='id int, label string, b boolean')

        output = source.collectToDataframe()

        print(output)
        print(output.dtypes)
        print(type(output["b"][1]))

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(elapsed_time)
        self.assertTrue(elapsed_time < 15)

    def test_date_format(self):
        import datetime
        data = pd.DataFrame([
            [0, datetime.datetime.fromisoformat('2021-11-01 00:00:00'), 100.0],
            [0, datetime.datetime.fromisoformat('2021-11-02 00:00:00'), 100.0],
            [0, datetime.datetime.fromisoformat('2021-11-03 00:00:00'), 100.0],
            [0, datetime.datetime.fromisoformat('2021-11-04 00:00:00'), 100.0],
            [0, datetime.datetime.fromisoformat('2021-11-05 00:00:00'), 100.0]
        ])
        source = dataframeToOperator(data, schemaStr='id int, ts timestamp, val double', op_type='batch')
        df = source.collectToDataframe()
        self.assertFalse(df.iloc[1]['ts'] is pd.NaT)
