from abc import ABC

from py4j.java_gateway import JavaObject
from pyflink.table.catalog import *

from ..bases.j_obj_wrapper import JavaObjectWrapper
from ..bases.params import Params
from ..conversion.java_method_call import auto_convert_java_type, call_java_method

__all__ = ['BaseCatalog', 'HiveCatalog', 'DerbyCatalog', 'MySqlCatalog', 'SqliteCatalog', 'OdpsCatalog']


class BaseCatalog(Catalog, JavaObjectWrapper, ABC):
    _j_cls_name = 'com.alibaba.alink.common.io.catalog.BaseCatalog'

    def __init__(self, j_catalog):
        super(BaseCatalog, self).__init__(j_catalog)

    def get_j_obj(self) -> JavaObject:
        return self._j_catalog

    @auto_convert_java_type
    def open(self):
        self.open()

    @auto_convert_java_type
    def close(self):
        self.close()

    @classmethod
    def of(cls, params) -> 'BaseCatalog':
        return call_java_method(cls._j_cls().of, params)

    @auto_convert_java_type
    def getParams(self) -> Params:
        return self.getParams()


class JdbcCatalog(BaseCatalog, ABC):
    _j_cls_name = 'com.alibaba.alink.common.io.catalog.JdbcCatalog'

    def __init__(self, *args):
        """
        Construct `JdbcCatalog` from arguments with a wrapped Java instance.
        Different combinations of arguments are supported:

        1. j_obj: JavaObject -> directly wrap the instance;
        2. params: Params -> call `JdbcCatalog(Params params)` of Java side;

        :param args: arguments, see function description.
        """
        if len(args) == 1 and isinstance(args[0], JavaObject):
            j_obj = args[0]
        else:
            j_obj = call_java_method(self._j_cls(), *args).get_j_obj()
        super(JdbcCatalog, self).__init__(j_obj)


class HiveCatalog(BaseCatalog):
    _j_cls_name = 'com.alibaba.alink.common.io.catalog.HiveCatalog'

    def __init__(self, *args):
        """
        Construct `HiveCatalog` from arguments with a wrapped Java instance.
        Different combinations of arguments are supported:

        1. j_obj: JavaObject -> directly wrap the instance;
        2. params: Params -> call `HiveCatalog(Params params)` of Java side;
        3. catalogName: str, defaultDatabase: str, hiveVersion: str hiveConfDir: str -> call `HiveCatalog(String catalogName, String defaultDatabase, String hiveVersion, String hiveConfDir)` of Java side.
        4. catalogName: str, defaultDatabase: str, hiveVersion: str, hiveConfDir: FilePath -> call `HiveCatalog(String catalogName, String defaultDatabase, String hiveVersion, FilePath hiveConfDir)` of Java side.
        5. catalogName: str, defaultDatabase: str, hiveVersion: str, hiveConfDir: str, kerberosPrincipal: str, kerberosKeytab: str -> call `HiveCatalog(String catalogName, String defaultDatabase, String hiveVersion, String hiveConfDir, String kerberosPrincipal, String kerberosKeytab)` of Java side.
        6. catalogName: str, defaultDatabase: str, hiveVersion: str, hiveConfDir: FilePath, kerberosPrincipal: str, kerberosKeytab: str -> call `HiveCatalog(String catalogName, String defaultDatabase, String hiveVersion, FilePath hiveConfDir, String kerberosPrincipal, String kerberosKeytab)` of Java side.

        :param args: arguments, see function description.
        """
        if len(args) == 1 and isinstance(args[0], JavaObject):
            j_obj = args[0]
        else:
            j_obj = call_java_method(self._j_cls(), *args).get_j_obj()
        super(BaseCatalog, self).__init__(j_obj)


class DerbyCatalog(JdbcCatalog):
    _j_cls_name = 'com.alibaba.alink.common.io.catalog.DerbyCatalog'

    def __init__(self, *args):
        """
        Construct `DerbyCatalog` from arguments with a wrapped Java instance.
        Different combinations of arguments are supported:

        1. j_obj: JavaObject -> directly wrap the instance;
        2. params: Params -> call `DerbyCatalog(Params params)` of Java side;
        3. catalogName: str, defaultDatabase: str, derbyVersion: str, derbyPath: str -> call `DerbyCatalog(String catalogName, String defaultDatabase, String derbyVersion, String derbyPath)` of Java side.
        4. catalogName: str, defaultDatabase: str, derbyVersion: str, derbyPath: str, userName: str, password: str -> call `DerbyCatalog(String catalogName, String defaultDatabase, String derbyVersion, String derbyPath, String userName, String password)` of Java side.

        :param args: arguments, see function description.
        """
        if len(args) == 1 and isinstance(args[0], JavaObject):
            j_obj = args[0]
        else:
            j_obj = call_java_method(self._j_cls(), *args).get_j_obj()
        super(DerbyCatalog, self).__init__(j_obj)


class MySqlCatalog(JdbcCatalog):
    _j_cls_name = 'com.alibaba.alink.common.io.catalog.MySqlCatalog'

    def __init__(self, *args):
        """
        Construct `MySqlCatalog` from arguments with a wrapped Java instance.
        Different combinations of arguments are supported:

        1. j_obj: JavaObject -> directly wrap the instance;
        2. params: Params -> call `MySqlCatalog(Params params)` of Java side;
        3. catalogName: str, defaultDatabase: str, mysqlVersion: str, mysqlUrl: str, port: str ->
        call `MySqlCatalog(String catalogName, String defaultDatabase, String mysqlVersion, String mysqlUrl, String port)` of Java side.
        4. catalogName: str, defaultDatabase: str, mysqlVersion: str, mysqlUrl: str, port: str, username: str, password: str ->
        call `MySqlCatalog(String catalogName, String defaultDatabase, String mysqlVersion, String mysqlUrl, String port, String userName, String password)` of Java side.

        :param args: arguments, see function description.
        """
        if len(args) == 1 and isinstance(args[0], JavaObject):
            j_obj = args[0]
        else:
            j_obj = call_java_method(self._j_cls(), *args).get_j_obj()
        super(MySqlCatalog, self).__init__(j_obj)


class SqliteCatalog(JdbcCatalog):
    _j_cls_name = 'com.alibaba.alink.common.io.catalog.SqliteCatalog'

    def __init__(self, *args):
        """
        Construct `SqliteCatalog` from arguments with a wrapped Java instance.
        Different combinations of arguments are supported:

        1. j_obj: JavaObject -> directly wrap the instance;
        2. params: Params -> call `SqliteCatalog(Params params)` of Java side;
        3. catalogName: str, defaultDatabase: str, sqliteVersion: str, dbUrls: List[str] ->
        call `SqliteCatalog(String catalogName, String defaultDatabase, String sqliteVersion, String[] dbUrls)` of Java side.
        4. catalogName: str, defaultDatabase: str, sqliteVersion: str, dbUrls: List[str], username: str, password: str ->
        call `SqliteCatalog(String catalogName, String defaultDatabase, String sqliteVersion, String[] dbUrls, String username, String password)` of Java side.

        :param args: arguments, see function description.
        """
        if len(args) == 1 and isinstance(args[0], JavaObject):
            j_obj = args[0]
        else:
            j_obj = call_java_method(self._j_cls(), *args).get_j_obj()
        super(SqliteCatalog, self).__init__(j_obj)


class InputOutputFormatCatalog(BaseCatalog):
    _j_cls_name = 'com.alibaba.alink.common.io.catalog.InputOutputFormatCatalog'


class OdpsCatalog(InputOutputFormatCatalog):
    _j_cls_name = 'com.alibaba.alink.common.io.catalog.OdpsCatalog'

    def __init__(self, *args):
        if len(args) == 1 and isinstance(args[0], JavaObject):
            j_obj = args[0]
        else:
            j_obj = call_java_method(self._j_cls(), *args).get_j_obj()
        super(OdpsCatalog, self).__init__(j_obj)
