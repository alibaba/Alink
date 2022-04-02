import os
import tempfile
import unittest

from pyflink.table.catalog import ObjectPath

from pyalink.alink import *


class TestCatalogObject(unittest.TestCase):

    def get_catalog_object(self):
        tempdir = tempfile.TemporaryDirectory().name
        os.makedirs(tempdir, exist_ok=True)
        url = os.path.join(tempdir, "test_sqlite_db")
        sqlite = SqliteCatalog("sqlite_test_catalog", "test_sqlite_db", "3.19.3", url)
        object_path = ObjectPath("test_sqlite_db", "test_table")
        catalog_object = CatalogObject(sqlite, object_path)
        return sqlite, catalog_object

    def test_constructor(self):
        _, catalog_object = self.get_catalog_object()
        self.assertEqual(SqliteCatalog, type(catalog_object.getCatalog()))
        self.assertEqual(ObjectPath, type(catalog_object.getObjectPath()))
        self.assertEqual(Params, type(catalog_object.getParams()))

    def test_serde(self):
        _, catalog_object = self.get_catalog_object()
        ser = catalog_object.serialize()
        print(ser)
        catalog_object2 = CatalogObject.deserialize(ser)
        print(catalog_object2)
        self.assertEqual(catalog_object.serialize(), catalog_object2.serialize())

    def test_source_sink(self):
        catalog, catalog_object = self.get_catalog_object()

        csv_source = CsvSourceBatchOp() \
            .setSchemaStr(
            "sepal_length double, sepal_width double, petal_length double, petal_width double, category string") \
            .setFilePath("https://alink-test-data.oss-cn-hangzhou.aliyuncs.com/iris.csv")
        CatalogSinkBatchOp() \
            .setCatalogObject(catalog_object) \
            .linkFrom(csv_source)
        BatchOperator.execute()

        source = CatalogSourceBatchOp().setCatalogObject(catalog_object)
        self.assertEqual(csv_source.getColNames(), source.getColNames())
        self.assertEqual(csv_source.getColTypes(), source.getColTypes())
