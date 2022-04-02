from py4j.java_gateway import JavaObject
from pyflink.table.catalog import ObjectPath

# noinspection PyProtectedMember
from .catalog import BaseCatalog
from ..bases.j_obj_wrapper import JavaObjectWrapperWithAutoTypeConversion
from ..bases.params import Params
from ..conversion.java_method_call import call_java_method

__all__ = ['CatalogObject', 'ObjectPath']


class CatalogObject(JavaObjectWrapperWithAutoTypeConversion):
    _j_cls_name = 'com.alibaba.alink.params.io.HasCatalogObject$CatalogObject'

    def __init__(self, *args):
        """
        Construct `CatalogObject` from arguments with a wrapped Java instance.
        Different combinations of arguments are supported:

        1. j_obj: JavaObject -> directly wrap the instance;
        2. catalog: BaseCatalog, objectPath: ObjectPath -> call `CatalogObject(BaseCatalog catalog, ObjectPath objectPath)` of Java side;
        3. catalog: BaseCatalog, objectPath: ObjectPath, params: Params -> call `CatalogObject(BaseCatalog catalog, ObjectPath objectPath, Params params)` of Java side;

        :param args: arguments, see function description.
        """
        if len(args) == 1 and isinstance(args[0], JavaObject):
            j_obj = args[0]
        else:
            j_obj = call_java_method(self._j_cls(), *args).get_j_obj()
        self._j_obj = j_obj

    def get_j_obj(self) -> JavaObject:
        return self._j_obj

    def getCatalog(self) -> BaseCatalog:
        return self.getCatalog()

    def getObjectPath(self) -> ObjectPath:
        return self.getObjectPath()

    def getParams(self) -> Params:
        return self.getParams()

    def serialize(self) -> str:
        return self.serialize()

    @classmethod
    def deserialize(cls, s: str) -> 'CatalogObject':
        return call_java_method(cls._j_cls().deserialize, s)
