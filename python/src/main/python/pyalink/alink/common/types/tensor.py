import io
import struct
from abc import ABC
from enum import Enum
from typing import List, Any, Optional, Sequence

import numpy
import numpy as np
from py4j.java_gateway import JavaObject

from .bases.j_obj_wrapper import JavaObjectWrapper
from .conversion.java_method_call import auto_convert_java_type, call_java_method
from .conversion.type_converters import get_all_subclasses
from .data_type_display import DataTypeDisplay
from ...py4j_util import get_java_class, is_java_instance

__all__ = ['Tensor', 'FloatTensor', 'DoubleTensor', 'IntTensor', 'LongTensor', 'BoolTensor',
           'ByteTensor', 'UByteTensor', 'StringTensor']


class DataType(Enum):
    FLOAT = 1
    DOUBLE = 2
    INT = 3
    LONG = 4
    BOOLEAN = 5
    BYTE = 6
    UBYTE = 7
    STRING = 8


TENSOR_WRAPPER_CLASS_NAME = "com.alibaba.alink.common.pyrunner.fn.conversion.TensorWrapper"


class Tensor(JavaObjectWrapper, DataTypeDisplay, ABC):
    _j_cls_name = 'com.alibaba.alink.common.linalg.tensor.Tensor'

    def __init__(self, j_tensor: JavaObject):
        self._arr: Optional[numpy.ndarray] = None
        self._j_tensor = j_tensor
        self._update_ndarray()

    def get_j_obj(self) -> JavaObject:
        return self._j_tensor

    def _update_ndarray(self):
        j_tensor_wrapper_cls = get_java_class(TENSOR_WRAPPER_CLASS_NAME)
        j_tensor_wrapper = j_tensor_wrapper_cls.fromJava(self.get_j_obj())
        buffer, dtype_str, shape = j_tensor_wrapper.getBytes(), j_tensor_wrapper.getDtypeStr(), j_tensor_wrapper.getShape()
        dtype = np.dtype(dtype_str)
        if dtype == np.bool:
            packed: numpy.ndarray = np.frombuffer(buffer, dtype='<B')
            unpacked = np.unpackbits(packed, count=np.prod(list(shape)).item(), bitorder='little')
            self._arr = unpacked.reshape(shape)
        elif dtype.type == np.str_:
            strs = []
            with io.BytesIO(buffer) as bytesio:
                size, = struct.unpack('>i', bytesio.read(4))
                for i in range(size):
                    length, = struct.unpack('>i', bytesio.read(4))
                    buffer = bytesio.read(length)
                    s = buffer.decode(encoding="UTF-8")
                    strs.append(s)
            self._arr = np.array(strs, dtype=np.object).reshape(shape)
        else:
            self._arr = np.frombuffer(buffer, dtype=dtype).reshape(shape)

    @staticmethod
    def fromNdarray(arr: numpy.ndarray):
        j_tensor_wrapper_cls = get_java_class(TENSOR_WRAPPER_CLASS_NAME)
        if arr.dtype == np.bool:
            packed = np.packbits(arr, bitorder='little')
            j_tensor_wrapper = j_tensor_wrapper_cls.fromPy(packed.tobytes(), arr.dtype.str, arr.shape)
        elif arr.dtype.type == np.str_:
            size: int = np.prod(arr.shape).item()
            with io.BytesIO() as bytesio:
                # Use big-endian, as bytes are read with DataInputStream in Java side
                bytesio.write(size.to_bytes(4, byteorder='big'))
                for s in arr.flatten():
                    bytesio.write(len(s).to_bytes(4, byteorder='big'))
                    bytesio.write(s.encode(encoding='UTF-8'))
                bytesio.flush()
                buffer = bytesio.getvalue()
            j_tensor_wrapper = j_tensor_wrapper_cls.fromPy(buffer, '<U', arr.shape)
        else:
            j_tensor_wrapper = j_tensor_wrapper_cls.fromPy(arr.tobytes(), arr.dtype.str, arr.shape)
        subclasses = get_all_subclasses(Tensor)
        subclasses.remove(NumericalTensor)
        j_tensor = j_tensor_wrapper.getJavaObject()
        for subclass in subclasses:
            # noinspection PyProtectedMember
            if is_java_instance(j_tensor, subclass._j_cls_name):
                return subclass(j_tensor)

    def toNdarray(self) -> numpy.ndarray:
        return self._arr

    @auto_convert_java_type
    def shape(self) -> List[int]:
        return self.shape()

    @auto_convert_java_type
    def size(self) -> int:
        return self.size()

    @auto_convert_java_type
    def getObject(self, coords: Sequence[int]) -> Any:
        """
        Get an entry by coordinates.

        :param coords: coordinates.
        :return: entry.
        """
        return self.getObject(coords)

    def setObject(self, v: Any, coords: Sequence[int]) -> 'Tensor':
        """
        Set an entry by coordinates.

        :param v: new value.
        :param coords: coordinates.
        :return: `self`.
        """
        call_java_method(self.get_j_obj().setObject, v, coords)
        self._update_ndarray()
        return self

    def reshape(self, shape: Sequence[int]) -> 'Tensor':
        """
        Reshape tensor to a new one.

        :param shape: new shape dimensions.
        :return: the new tensor.
        """
        j_shape_cls = get_java_class("com.alibaba.alink.common.linalg.tensor.Shape")
        j_shape = call_java_method(j_shape_cls, shape)
        return call_java_method(self.get_j_obj().reshape, j_shape)

    @auto_convert_java_type
    def flatten(self, startDim: Optional[int] = 0, endDim: Optional[int] = -1) -> 'Tensor':
        """
        Flatten consecutive dimensions of `self` to a new one.
        Use `startDim` and `endDim` to specify the index range of dimensions to be flattened. Negative values can be used, for example, -1 means last dimension index, and so on.

        :param startDim: start dimension, inclusive.
        :param endDim: end dimension, inclusive.
        :return: the new tensor.
        """
        if (startDim is None) or (endDim is None):
            return self.flatten()
        else:
            return self.flatten(startDim, endDim)

    @staticmethod
    def stack(tensors: Sequence['Tensor'], dim: int) -> 'Tensor':
        """
        Stack several tensors along a given dimension.

        :param tensors: a list of tensors.
        :param dim: the dimension to stack along.
        :return: the stacked tensor.
        """
        return call_java_method(Tensor._j_cls().stack, tensors, dim, None)

    @staticmethod
    def unstack(tensor: 'Tensor', dim: int) -> List['Tensor']:
        """
        Unstack a tensor to several tensors along a given dimension.

        :param tensor: the tensor to be unstacked.
        :param dim: the dimension to unstack along.
        :return: a list of tensors.
        """
        return call_java_method(Tensor._j_cls().unstack, tensor, dim, None)

    def getType(self) -> DataType:
        """
        Get data type.

        :return: data type.
        """
        return DataType(self.get_j_obj().getType().getIndex())

    @auto_convert_java_type
    def toDisplayData(self, n: int = None):
        if n is None:
            return self.toDisplayData()
        else:
            return self.toDisplayData(n)

    @auto_convert_java_type
    def toDisplaySummary(self) -> str:
        return self.toDisplaySummary()

    @auto_convert_java_type
    def toShortDisplayData(self) -> str:
        return self.toShortDisplayData()

    _unsupported_j_methods = ['permute', 'cat']


class NumericalTensor(Tensor, ABC):

    @auto_convert_java_type
    def min(self, dim: int, keepDim: bool) -> 'NumericalTensor':
        """
        Calculate minimum value along dimension.

        :param dim: dimension.
        :param keepDim: whether to keep the dimension as length 1.
        :return: the result tensor.
        """
        return self.min(dim, keepDim)

    @auto_convert_java_type
    def max(self, dim: int, keepDim: bool) -> 'NumericalTensor':
        """
        Calculate maximum value along dimension.

        :param dim: dimension.
        :param keepDim: whether to keep the dimension as length 1.
        :return: the result tensor.
        """
        return self.max(dim, keepDim)

    @auto_convert_java_type
    def sum(self, dim: int, keepDim: bool) -> 'NumericalTensor':
        """
        Calculate sum value along dimension.

        :param dim: dimension.
        :param keepDim: whether to keep the dimension as length 1.
        :return: the result tensor.
        """
        return self.sum(dim, keepDim)

    @auto_convert_java_type
    def mean(self, dim: int, keepDim: bool) -> 'NumericalTensor':
        """
        Calculate mean value along dimension.

        :param dim: dimension.
        :param keepDim: whether to keep the dimension as length 1.
        :return: the result tensor.
        """
        return self.mean(dim, keepDim)


class FloatTensor(NumericalTensor):
    _j_cls_name = 'com.alibaba.alink.common.linalg.tensor.FloatTensor'

    @auto_convert_java_type
    def getFloat(self, coords: Sequence[int]) -> float:
        """
        Get an entry by coordinates.
        """
        return self.getFloat(coords)

    @auto_convert_java_type
    def setFloat(self, v: float, coords: Sequence[int]) -> 'FloatTensor':
        """
        Set an entry by coordinates.
        """
        return self.setFloat(v, coords)


class DoubleTensor(NumericalTensor):
    _j_cls_name = 'com.alibaba.alink.common.linalg.tensor.DoubleTensor'

    @auto_convert_java_type
    def getDouble(self, coords: Sequence[int]) -> float:
        """
        Get an entry by coordinates.
        """
        return self.getDouble(coords)

    @auto_convert_java_type
    def setDouble(self, v: float, coords: Sequence[int]) -> 'DoubleTensor':
        """
        Set an entry by coordinates.
        """
        return self.setDouble(v, coords)


class IntTensor(NumericalTensor):
    _j_cls_name = 'com.alibaba.alink.common.linalg.tensor.IntTensor'

    @auto_convert_java_type
    def getInt(self, coords: Sequence[int]) -> int:
        """
        Get an entry by coordinates.
        """
        return self.getInt(coords)

    @auto_convert_java_type
    def setInt(self, v: int, coords: Sequence[int]) -> 'IntTensor':
        """
        Set an entry by coordinates.
        """
        return self.setInt(v, coords)


class LongTensor(NumericalTensor):
    _j_cls_name = 'com.alibaba.alink.common.linalg.tensor.LongTensor'

    @auto_convert_java_type
    def getLong(self, coords: Sequence[int]) -> int:
        """
        Get an entry by coordinates.
        """
        return self.getLong(coords)

    @auto_convert_java_type
    def setLong(self, v: int, coords: Sequence[int]) -> 'LongTensor':
        """
        Set an entry by coordinates.
        """
        return self.setLong(v, coords)


class BoolTensor(Tensor):
    _j_cls_name = 'com.alibaba.alink.common.linalg.tensor.BoolTensor'

    @auto_convert_java_type
    def getBoolean(self, coords: Sequence[int]) -> bool:
        """
        Get an entry by coordinates.
        """
        return self.getBoolean(coords)

    @auto_convert_java_type
    def setBoolean(self, v: bool, coords: Sequence[int]) -> 'BoolTensor':
        """
        Set an entry by coordinates.
        """
        return self.setBoolean(v, coords)


class ByteTensor(Tensor):
    _j_cls_name = 'com.alibaba.alink.common.linalg.tensor.ByteTensor'

    @auto_convert_java_type
    def getByte(self, coords: Sequence[int]) -> int:
        """
        Get an entry by coordinates.
        """
        return self.getByte(coords)

    @auto_convert_java_type
    def setByte(self, v: int, coords: Sequence[int]) -> 'ByteTensor':
        """
        Set an entry by coordinates.
        """
        return self.setByte(v, coords)


class UByteTensor(Tensor):
    _j_cls_name = 'com.alibaba.alink.common.linalg.tensor.UByteTensor'

    @auto_convert_java_type
    def getUByte(self, coords: Sequence[int]) -> int:
        """
        Get an entry by coordinates.
        """
        return self.getUByte(coords)

    @auto_convert_java_type
    def setUByte(self, v: int, coords: Sequence[int]) -> 'UByteTensor':
        """
        Set an entry by coordinates.
        """
        return self.setUByte(v, coords)


class StringTensor(Tensor):
    _j_cls_name = 'com.alibaba.alink.common.linalg.tensor.StringTensor'

    @auto_convert_java_type
    def getString(self, coords: Sequence[int]) -> str:
        """
        Get an entry by coordinates.
        """
        return self.getString(coords)

    @auto_convert_java_type
    def setString(self, v: str, coords: Sequence[int]) -> 'StringTensor':
        """
        Set an entry by coordinates.
        """
        return self.setString(v, coords)
