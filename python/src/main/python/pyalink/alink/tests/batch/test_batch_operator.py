import unittest

from pyalink.alink import *


class TestBatchOperator(unittest.TestCase):

    def test_firstN(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        sample = source.firstN(50)
        df = sample.collectToDataframe()
        self.assertEqual(len(df), 50)

    def test_sample(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        sample = source.sample(0.1)
        df = sample.collectToDataframe()
        self.assertTrue(abs(len(df) - 150 * 0.1) <= 15)

    def test_sampleWithSize(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        sample = source.sampleWithSize(50, True)
        df = sample.collectToDataframe()
        self.assertEqual(len(df), 50)

    def test_select(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        res = source.select(["sepal_length", "sepal_width"])
        df = res.collectToDataframe()
        print(df)
        self.assertEqual(len(df), 150)
        self.assertEqual(len(df.columns), 2)

    def test_select2(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        res = source.select("sepal_length, sepal_width as f2")
        df = res.collectToDataframe()
        print(df)
        self.assertEqual(len(df), 150)
        self.assertEqual(len(df.columns), 2)

    def test_alias(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        res = source.select(["sepal_length", "sepal_width"]).alias(["f0", "f1"])
        df = res.collectToDataframe()
        print(df)
        self.assertEqual(len(df), 150)
        self.assertEqual(len(df.columns), 2)

    def test_alias2(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        res = source.select(["sepal_length", "sepal_width"]).alias("f0, f1")
        df = res.collectToDataframe()
        print(df)
        self.assertEqual(len(df), 150)
        self.assertEqual(len(df.columns), 2)

    def test_select_stream(self):
        stream_source = CsvSourceStreamOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        res = stream_source.select(["petal_length", "sepal_length"])
        res.print()
        StreamOperator.execute()

    def test_where(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        res = source.where("sepal_length < 0.5")
        df = res.collectToDataframe()
        res.print()
        self.assertTrue(len(df) < 1000)
        self.assertEqual(len(df.columns), 5)

    def test_filter(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        res = source.filter("sepal_length < 0.5")
        df = res.collectToDataframe()
        res.print()
        self.assertTrue(len(df) < 1000)
        self.assertEqual(len(df.columns), 5)

    def test_distinct(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        import numpy as np
        import pandas as pd
        arr = np.array([
            [1, 2, 3],
            [1, 2, 3],
            [3, 4, 5]
        ])
        df = pd.DataFrame(arr)
        source = dataframeToOperator(df, schemaStr="col0 int, col1 int, col2 int", opType="batch")
        res = source.distinct()
        df_res = res.collectToDataframe()
        self.assertEqual(len(df_res), 2)
        self.assertEqual(len(df_res.columns), 3)

    def test_orderBy(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        import numpy as np
        import pandas as pd
        arr = np.array([
            [1, 2, 3],
            [1, 2, 3],
            [3, 4, 5],
            [1, 2, 1]
        ])
        df = pd.DataFrame(arr)
        source = dataframeToOperator(df, schemaStr="col0 int, col1 int, col2 int", opType="batch")
        res = source.orderBy("col2", limit=3, order="desc")
        res.print()
        df_res = res.collectToDataframe()
        self.assertEqual(len(df_res), 3)
        self.assertEqual(len(df_res.columns), 3)

        res = source.orderBy("col2", offset=3, fetch=1)
        res.print()
        df_res = res.collectToDataframe()
        self.assertEqual(len(df_res), 1)
        self.assertEqual(len(df_res.columns), 3)

    def test_groupBy(self):
        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        import numpy as np
        import pandas as pd
        arr = np.array([
            [1, 2, 3],
            [1, 2, 3],
            [3, 4, 5],
            [1, 2, 1]
        ])
        df = pd.DataFrame(arr)
        source = dataframeToOperator(df, schemaStr="col0 int, col1 int, col2 int", opType="batch")
        res = source.groupBy(by="col1", select="col1, sum(col2),sum(col0)")
        res.print()
        df_res = res.collectToDataframe()
        self.assertEqual(len(df_res), 2)
        self.assertEqual(len(df_res.columns), 3)

    def test_rebalance(self):
        import numpy as np
        import pandas as pd
        arr = np.array([
            [1, 2, 3],
            [1, 2, 3],
            [3, 4, 5],
            [1, 2, 1]
        ])
        df = pd.DataFrame(arr)
        source = dataframeToOperator(df, schemaStr="col0 int, col1 int, col2 int", opType="batch")
        res = source.rebalance()
        df_res = res.collectToDataframe()
        self.assertEqual(len(df_res), 4)
        self.assertEqual(len(df_res.columns), 3)

    def test_shuffle(self):
        import numpy as np
        import pandas as pd
        arr = np.array([
            [1, 2, 3],
            [1, 2, 3],
            [3, 4, 5],
            [1, 2, 1]
        ])
        df = pd.DataFrame(arr)
        source = dataframeToOperator(df, schemaStr="col0 int, col1 int, col2 int", opType="batch")
        res = source.shuffle()
        df_res = res.collectToDataframe()
        self.assertEqual(len(df_res), 4)
        self.assertEqual(len(df_res.columns), 3)

    def test_collectStatistics(self):
        import numpy as np
        import pandas as pd
        arr = np.array([
            [1, 2, 3],
            [1, 2, 3],
            [3, 4, 5],
            [1, 2, 1]
        ])
        df = pd.DataFrame(arr)
        source = dataframeToOperator(df, schemaStr="col0 int, col1 int, col2 int", opType="batch")
        summary = source.collectStatistics()
        self.assertEqual(TableSummary, type(summary))
        self.assertEqual(summary.count(), 4)

    def test_print(self):
        import io
        import sys

        saved_stdout = sys.stdout
        str_out = io.StringIO()
        sys.stdout = str_out

        source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        title = "===== PRINT ====="
        source.print(100, title)
        content = str_out.getvalue()
        self.assertTrue(title in content)

        sys.stdout = saved_stdout
        print(str_out.getvalue())
