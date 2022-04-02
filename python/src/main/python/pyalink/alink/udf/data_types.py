from abc import ABC

from pyflink.table import DataTypes
# noinspection PyProtectedMember
from pyflink.table.types import DataType


class AlinkDataType(DataType, ABC):
    @classmethod
    def to_type_string(cls):
        """
        Flink type string of this type
        """
        raise NotImplementedError("AlinkDataType must implement module().")


class TensorType(AlinkDataType):
    @classmethod
    def to_type_string(cls):
        return "TENSOR"


class VectorType(AlinkDataType):
    @classmethod
    def to_type_string(cls):
        return "VECTOR"


class MTableType(AlinkDataType):
    @classmethod
    def to_type_string(cls):
        return "MTABLE"


class AlinkDataTypes(DataTypes):
    @staticmethod
    def TENSOR() -> TensorType:
        return TensorType()

    @staticmethod
    def VECTOR() -> VectorType:
        return VectorType()

    @staticmethod
    def MTABLE() -> MTableType:
        return MTableType()
