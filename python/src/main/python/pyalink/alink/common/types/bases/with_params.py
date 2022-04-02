from abc import ABC

from .j_obj_wrapper import JavaObjectWrapper


class WithParams(JavaObjectWrapper, ABC):

    def __init__(self, *args, **kwargs):
        self.params = dict()

    def _add_param(self, key, val):
        from ..file_system.file_system import FilePath
        from ..catalog.catalog_object import CatalogObject
        if isinstance(val, (FilePath, CatalogObject, )):
            self.params[key] = val.serialize()
        else:
            self.params[key] = val
        method_name = "set" + key[:1].upper() + key[1:]
        j_func = self.get_j_obj().__getattr__(method_name)

        from ..conversion.java_method_call import call_java_method
        call_java_method(j_func, val)
        return self
