from py4j.java_gateway import JavaObject

from ..bases.j_obj_wrapper import JavaObjectWrapperWithFunc
from ..conversion.java_method_call import call_java_method
from ....py4j_util import get_java_class

__all__ = ['InputStreamWrapper']


class InputStreamWrapper(JavaObjectWrapperWithFunc):
    _j_cls_name = "java.io.InputStream"

    def __init__(self, j_obj):
        self._j_input_stream = j_obj

    def get_j_obj(self) -> JavaObject:
        return self._j_input_stream

    def read(self, length=1, offset=0):
        data_stream_read_util_cls = get_java_class("com.alibaba.alink.python.utils.DataStreamReadUtil")
        (numBytesRead, b) = call_java_method(data_stream_read_util_cls.read, self, length, offset)
        return numBytesRead, b
