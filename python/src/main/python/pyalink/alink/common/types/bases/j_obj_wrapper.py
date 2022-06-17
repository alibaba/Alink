from abc import ABC, abstractmethod, ABCMeta

from py4j.java_gateway import JavaObject, JavaClass

from ..conversion.java_method_call import auto_convert_java_type
from ..conversion.type_converters import j_value_to_py_value
from ....py4j_util import get_java_class

__all__ = ['JavaObjectWrapper', 'JavaObjectWrapperWithFunc', 'JavaObjectWrapperWithAutoTypeConversion']


class JavaObjectWrapper(ABC):
    """
    A wrapped Java object.
    The Java object is represented as a :py:class:`py4j.java_gateway.JavaObject`.
    Refer to `Py4J documentation <https://www.py4j.org/contents.html>`_ for more information.
    """

    _j_cls_name: str

    @abstractmethod
    def get_j_obj(self) -> JavaObject:
        """
        Get the wrapped Java object.

        :return: the wrapped Java object.
        """
        ...

    @classmethod
    def _j_cls(cls) -> JavaClass:
        return get_java_class(cls._j_cls_name)

    def __str__(self) -> str:
        return self.get_j_obj().toString()


class JavaObjectWrapperWithFunc(JavaObjectWrapper, ABC):
    """
    A wrapped Java object with the capability to call its Java methods directly.

    When calling the Java method, if arguments are of type :class:`JavaObjectWrapper`,
    the return values of :py:func:`get_j_obj` are passed.
    Otherwise, themselves are passed as they are, where Py4J will do simple conversion.
    The returned value is automatically converted to Python value through :py:func:`pyalink.alink.common.types.conversion.type_converters.j_value_to_py_value`.
    """

    def __dir__(self):
        keys = self.get_j_obj().__dir__()
        return keys

    def __getattr__(self, attr_name):
        def wrapped_func(f):
            def inner(*args, **kwargs):
                args = [
                    arg.get_j_obj() if isinstance(arg, JavaObjectWrapper) else arg
                    for arg in args
                ]
                retval = f(*args, **kwargs)
                return j_value_to_py_value(retval)

            return inner

        # assume all access are functions
        attr = self.get_j_obj().__getattr__(attr_name)
        if isinstance(attr, JavaObject):
            return j_value_to_py_value(attr)
        else:
            return wrapped_func(attr)


class AutoTypeConversionMetaClass(ABCMeta):
    def __new__(mcs, name, bases, attrs):
        exclude_attrs = ['get_j_obj', '_j_obj']
        for attr_name, attr_value in attrs.items():
            if not attr_name.startswith("__") and attr_name not in exclude_attrs and callable(attr_value):
                attrs[attr_name] = auto_convert_java_type(attr_value)
        return super(AutoTypeConversionMetaClass, mcs).__new__(mcs, name, bases, attrs)


class JavaObjectWrapperWithAutoTypeConversion(JavaObjectWrapper, ABC, metaclass=AutoTypeConversionMetaClass):
    """
    A wrapped Java object with the capability of automatically type conversion of arguments and return values for
    all instances methods.

    NOTE: Static methods and class methods are not included. Use the decorator :py:func:`pyalink.alink.common.types.conversion.java_method_call.auto_convert_java_type` instead.
    """
