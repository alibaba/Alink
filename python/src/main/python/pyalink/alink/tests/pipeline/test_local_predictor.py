import tempfile
import unittest
from typing import List

import numpy as np
import pandas as pd

from pyalink.alink import *


class TestLocalPredictor(unittest.TestCase):

    def test_create_input_j_row(self):
        j_csv_util_cls = get_java_class("com.alibaba.alink.common.utils.TableUtil")
        input_schema_str = "col0 boolean, col1 byte, col2 short, col3 int, col4 long, col5 float, col6 double, col7 varchar"
        values = [True, 1, 1, 1, 1, 2., 4., "123"]
        j_input_col_types: List[JavaObject] = j_csv_util_cls.getColTypes(input_schema_str)
        j_row = LocalPredictor._create_input_j_row(values, j_input_col_types)
        j_row_type_adapter = get_java_class("com.alibaba.alink.python.utils.RowTypeAdapter")
        j_row_type_adapter.checkRowType(j_row, j_input_col_types)

    def test_numeric_results(self):
        data = np.array([
            [2, 1, 1],
            [3, 2, 1],
            [4, 3, 2],
            [2, 4, 1],
            [2, 2, 1],
            [4, 3, 2],
            [1, 2, 1],
            [5, 3, 2]])
        df = pd.DataFrame({"f0": data[:, 0],
                           "f1": data[:, 1],
                           "label": data[:, 2]})

        dataset = dataframeToOperator(df, schemaStr='f0 double, f1 double, label int', op_type='batch')
        colnames = ["f0", "f1"]
        svm = LinearSvm().setFeatureCols(colnames).setLabelCol("label").setPredictionCol("pred")
        model: LinearSvmModel = svm.fit(dataset)

        predictor = model.collectLocalPredictor("f0 double, f1 double")

        self.assertListEqual(predictor.getOutputColNames(), ['f0', 'f1', 'pred'])
        self.assertListEqual(predictor.getOutputColTypes(), ['DOUBLE', 'DOUBLE', 'INT'])

        res = predictor.map([2., 2.])
        np.testing.assert_array_almost_equal(res,
                                             np.array([2., 2., 1], dtype=object))
        self.assertTrue(isinstance(res[0], float))
        self.assertTrue(isinstance(res[1], float))
        self.assertTrue(isinstance(res[2], int))

        res = predictor.map(np.array([2.4, 2.2]))
        np.testing.assert_array_almost_equal(res,
                                             np.array([2.4, 2.2, 1], dtype=object))

    def test_string_values(self):
        data = np.array([
            [0, 'That is an English Book!'],
            [1, 'Do you like math?'],
            [2, 'Have a good day!']
        ], dtype=object)

        tokenizer = Tokenizer().setSelectedCol("text").setOutputCol("output")
        predictor = tokenizer.collectLocalPredictor('id long, text string')

        self.assertListEqual(predictor.getOutputColNames(), ['id', 'text', 'output'])
        self.assertListEqual(predictor.getOutputColTypes(), ['BIGINT', 'VARCHAR', 'VARCHAR'])

        res = predictor.map([0, 'That is an English Book!'])
        np.testing.assert_array_equal(res,
                                      np.array([0, 'That is an English Book!', 'that is an english book!'], dtype=object))
        self.assertTrue(isinstance(res[0], int))
        self.assertTrue(isinstance(res[1], str))
        self.assertTrue(isinstance(res[2], str))

        res = predictor.map([1, 'Do you like math?'])
        np.testing.assert_array_equal(res,
                                      np.array([1, 'Do you like math?', 'do you like math?'], dtype=object))

    def test_load_model(self):
        schemaStr = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string"
        source = CsvSourceBatchOp() \
            .setSchemaStr(schemaStr) \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")

        model_filename = tempfile.NamedTemporaryFile().name

        stage1 = QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["sepal_length"])
        stage2 = Binarizer().setSelectedCol("petal_width").setThreshold(1.)
        stage3 = QuantileDiscretizer().setNumBuckets(4).setSelectedCols(["petal_length"])
        pipeline_model = Pipeline(stage1, stage2, stage3).fit(source)
        predictor = pipeline_model.collectLocalPredictor(schemaStr)

        self.assertListEqual(predictor.getOutputColNames(),
                             ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'category'])
        self.assertListEqual(predictor.getOutputColTypes(),
                             ['BIGINT', 'DOUBLE', 'BIGINT', 'DOUBLE', 'VARCHAR'])

        res = predictor.map([1.2, 3.4, 2.4, 3.6, "1"])
        np.testing.assert_array_equal(res,
                                      np.array([0, 3.4, 1, 1.0, "1"], dtype=object))
        res = predictor.map(np.array([1.2, 3.4, 2.4, 3.6, "1"], dtype=object))

    def test_load_model_str_path(self):
        schemaStr = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string"
        source = CsvSourceBatchOp() \
            .setSchemaStr(schemaStr) \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")

        model_filename = tempfile.NamedTemporaryFile().name

        stage1 = QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["sepal_length"])
        stage2 = Binarizer().setSelectedCol("petal_width").setThreshold(1.)
        stage3 = QuantileDiscretizer().setNumBuckets(4).setSelectedCols(["petal_length"])
        pipeline_model = Pipeline(stage1, stage2, stage3).fit(source)
        pipeline_model.save(model_filename)
        BatchOperator.execute()

        predictor = LocalPredictor(model_filename, schemaStr)
        self.assertListEqual(predictor.getOutputColNames(),
                             ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'category'])
        self.assertListEqual(predictor.getOutputColTypes(),
                             ['BIGINT', 'DOUBLE', 'BIGINT', 'DOUBLE', 'VARCHAR'])

        res = predictor.map([1.2, 3.4, 2.4, 3.6, "1"])
        np.testing.assert_array_equal(res,
                                      np.array([0, 3.4, 1, 1.0, "1"], dtype=object))
        res = predictor.map(np.array([1.2, 3.4, 2.4, 3.6, "1"], dtype=object))

    def test_load_model_file_path(self):
        schemaStr = "sepal_length double, sepal_width double, petal_length double, petal_width double, category string"
        source = CsvSourceBatchOp() \
            .setSchemaStr(schemaStr) \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")

        model_filename = tempfile.NamedTemporaryFile().name
        model_file_path = FilePath(model_filename, LocalFileSystem())

        stage1 = QuantileDiscretizer().setNumBuckets(2).setSelectedCols(["sepal_length"])
        stage2 = Binarizer().setSelectedCol("petal_width").setThreshold(1.)
        stage3 = QuantileDiscretizer().setNumBuckets(4).setSelectedCols(["petal_length"])
        pipeline_model = Pipeline(stage1, stage2, stage3).fit(source)
        pipeline_model.save(model_file_path)
        BatchOperator.execute()

        predictor = LocalPredictor(model_file_path, schemaStr)

        print(predictor.getOutputColTypes())
        print(predictor.getOutputColNames())
        # self.assertListEqual(predictor.getOutputColNames(),
        #                      ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'category'])
        # self.assertListEqual(predictor.getOutputColTypes(),
        #                      ['BIGINT', 'DOUBLE', 'BIGINT', 'DOUBLE', 'VARCHAR'])

        res = predictor.map([1.2, 3.4, 2.4, 3.6, "1"])
        # np.testing.assert_array_equal(res,
        #                               np.array([0, 3.4, 1, 1.0, "1"], dtype=object))
        res = predictor.map(np.array([1.2, 3.4, 2.4, 3.6, "1"], dtype=object))
