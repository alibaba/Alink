import os
import tempfile
import unittest

import pytest
from py4j.protocol import Py4JJavaError, Py4JError
from pyflink.table.catalog import CatalogDatabase

from pyalink.alink import *


class TestCatalog(unittest.TestCase):
    def test_base_catalog_of(self):
        url = os.path.join(tempfile.TemporaryDirectory().name, "derby_db")
        params = Params.from_args(
            ioName="derby",
            catalogName="derby_test_catalog",
            defaultDatabase="derby_schema",
            pluginVersion="10.6.1.0",
            url=url)
        derby = BaseCatalog.of(params)
        self.assertTrue(isinstance(derby, DerbyCatalog))
        derby.open()
        self.assertTrue(len(derby.list_databases()) > 0)
        derby.close()

        from_j_params = derby.getParams()
        self.assertEqual(len(params), len(from_j_params))
        for k, v in from_j_params.items():
            self.assertEqual(params[k], from_j_params[k])

    def check_derby_catalog(self, obj: DerbyCatalog):
        self.assertTrue(isinstance(obj, DerbyCatalog))
        obj.open()
        obj.create_database("derby_schema", CatalogDatabase(None), True)
        self.assertTrue(len(obj.list_databases()) > 0)
        obj.close()

    def test_create_derby_catalog(self):
        url = os.path.join(tempfile.TemporaryDirectory().name, "derby_db")
        params = Params.from_args(
            ioName="derby",
            catalogName="derby_test_catalog",
            defaultDatabase="derby_schema",
            pluginVersion="10.6.1.0",
            url=url)
        derby = DerbyCatalog(params)
        self.check_derby_catalog(derby)

        derby = DerbyCatalog("derby_test_catalog", "derby_schema", "10.6.1.0", url)
        self.check_derby_catalog(derby)

        derby = DerbyCatalog("derby_test_catalog", "derby_schema", "10.6.1.0", url, "xxx", "xxx")
        self.check_derby_catalog(derby)
        print(derby.getParams())

        with self.assertRaises(Py4JError):
            DerbyCatalog("derby_test_catalog", "derby_schema", "10.6.1.0", url, "xxx", "xxx", "xxx")

        with self.assertRaises(Py4JJavaError):
            derby = DerbyCatalog("derby_test_catalog", "derby_schema", url, "10.6.1.0")
            self.check_derby_catalog(derby)

    def check_sqlite_catalog(self, obj: SqliteCatalog):
        self.assertTrue(isinstance(obj, SqliteCatalog))
        obj.open()
        self.assertTrue(len(obj.list_databases()) > 0)
        obj.close()

    def test_create_sqlite_catalog(self):
        tempdir = tempfile.TemporaryDirectory().name
        os.makedirs(tempdir, exist_ok=True)
        url = os.path.join(tempdir, "test_sqlite_db")

        params = Params.from_args(
            ioName="sqlite",
            catalogName="sqlite_test_catalog",
            defaultDatabase="test_sqlite_db",
            pluginVersion="3.19.3",
            urls=[url])
        sqlite = SqliteCatalog(params)
        self.check_sqlite_catalog(sqlite)

        sqlite = SqliteCatalog("sqlite_test_catalog", "test_sqlite_db", "3.19.3", url)
        self.check_sqlite_catalog(sqlite)

        sqlite = SqliteCatalog("sqlite_test_catalog", "test_sqlite_db", "3.19.3", [url])
        self.check_sqlite_catalog(sqlite)

        sqlite = SqliteCatalog("sqlite_test_catalog", "test_sqlite_db", "3.19.3", [url], "xxx", "xxx")
        self.check_sqlite_catalog(sqlite)

        with self.assertRaises(Py4JError):
            SqliteCatalog("sqlite_test_catalog", "test_sqlite_db", "3.19.3", [url], "xxx", "xxx", "xxx")

        with self.assertRaises(Py4JJavaError):
            sqlite = SqliteCatalog("sqlite_test_catalog", "test_sqlite_db", url, "3.19.3")
            self.check_sqlite_catalog(sqlite)

    def check_mysql_catalog(self, obj: MySqlCatalog):
        self.assertTrue(isinstance(obj, MySqlCatalog))
        obj.open()
        self.assertTrue(len(obj.list_databases()) > 0)
        obj.close()

    @pytest.mark.skip()
    def test_create_mysql_catalog(self):
        username = "xxx"
        password = "xxx"

        params = Params.from_args(
            ioName="mysql",
            catalogName="test_mysql",
            defaultDatabase="mysql",
            pluginVersion="5.1.27",
            url="127.0.0.1",
            port="3306",
            username=username,
            password=password)
        mysql = MySqlCatalog(params)
        self.check_mysql_catalog(mysql)

        mysql = MySqlCatalog("test_mysql", "mysql", "5.1.27", "127.0.0.1", "3306", username, password)
        self.check_mysql_catalog(mysql)

        with self.assertRaises(Py4JError):
            mysql = MySqlCatalog("test_mysql", "mysql", "5.1.27", "127.0.0.1", "3306")
            self.check_mysql_catalog(mysql)

        with self.assertRaises(Py4JError):
            mysql = MySqlCatalog("test_mysql", "mysql", "5.1.27", "127.0.0.1")
            self.check_mysql_catalog(mysql)

    def check_odps_catalog(self, obj: OdpsCatalog):
        self.assertTrue(isinstance(obj, OdpsCatalog))
        obj.open()
        self.assertTrue(len(obj.list_databases()) > 0)
        obj.close()

    @pytest.mark.skip()
    def test_create_odps_catalog(self):
        accessId = "xxx"
        accessKey = "xxx"
        project = "xxx"
        endpoint = "xxx"

        params = Params.from_args(
            ioName="odps",
            catalogName="odps_test_catalog",
            defaultDatabase="odps_schema",
            pluginVersion="0.36.4-public",
            accessId=accessId,
            accessKey=accessKey,
            project=project,
            endPoint=endpoint,
            runningProject=None
        )
        odps = OdpsCatalog(params)
        self.check_odps_catalog(odps)

        odps = OdpsCatalog("odps_test_catalog", "odps_schema", "0.36.4-public",
                           accessId, accessKey, project, endpoint, None)
        self.check_odps_catalog(odps)

    def check_hive_catalog(self, obj: HiveCatalog):
        self.assertTrue(isinstance(obj, HiveCatalog))
        obj.open()
        self.assertTrue(len(obj.list_databases()) > 0)
        obj.close()

    @pytest.mark.skip()
    def test_create_hive_catalog(self):
        hiveConfDir = "xxx/apache-hive-2.3.4-bin/conf"
        hiveVersion = "2.3.4"
        hive = HiveCatalog("test_hive_catalog", None, hiveVersion, hiveConfDir)
        self.check_hive_catalog(hive)

        params = Params.from_args(
            ioName="hive",
            catalogName="test_hive_catalog",
            defaultDatabase="default",
            pluginVersion=hiveVersion,
            hiveConfDir=hiveConfDir)
        hive = HiveCatalog(params)
        self.check_hive_catalog(hive)

        with self.assertRaises(Py4JError):
            hive = HiveCatalog("test_hive_catalog", None, hiveVersion, hiveConfDir, "abc")
            self.check_hive_catalog(hive)
