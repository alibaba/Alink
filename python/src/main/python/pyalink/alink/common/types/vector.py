from abc import ABC

from py4j.java_gateway import JavaObject

from .bases.j_obj_wrapper import JavaObjectWrapperWithAutoTypeConversion
from .conversion.java_method_call import auto_convert_java_type, call_java_method
from .data_type_display import DataTypeDisplay

__all__ = ['Vector', 'DenseVector', 'SparseVector', 'DenseMatrix']


class Vector(JavaObjectWrapperWithAutoTypeConversion, ABC):
    """
    Vector
    """
    _j_cls_name = 'com.alibaba.alink.common.linalg.Vector'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def add(self, i, val):
        return self.add(i, val)

    def get(self, i):
        return self.get(i)

    def append(self, v):
        return self.append(v)

    def size(self):
        return self.size()

    def iterator(self):
        return self.iterator()

    def set(self, i, val):
        return self.set(i, val)

    def scale(self, v):
        return self.scale(v)

    def slice(self, indexes):
        return self.slice(indexes)

    def prefix(self, v):
        return self.prefix(v)

    def scaleEqual(self, v):
        return self.scaleEqual(v)

    def normL1(self):
        return self.normL1()

    def normInf(self):
        return self.normInf()

    def normL2(self):
        return self.normL2()

    def normL2Square(self):
        return self.normL2Square()

    def normalizeEqual(self, p):
        return self.normalizeEqual(p)

    def standardizeEqual(self, mean, stdvar):
        return self.standardizeEqual(mean, stdvar)

    def plus(self, vec):
        return self.plus(vec)

    def minus(self, vec):
        return self.minus(vec)

    def dot(self, vec):
        return self.dot(vec)

    def outer(self, other=None):
        if other is None:
            return self.outer()
        else:
            return self.outer(other)

    def clone(self):
        return self.clone()

    _unsupported_j_methods = ['toBytes']


class DenseVector(Vector, DataTypeDisplay):
    """
    DenseVector
    """
    _j_cls_name = 'com.alibaba.alink.common.linalg.DenseVector'

    def __init__(self, *args):
        """
        Construct `DenseVector` from arguments with a wrapped Java instance.
        Different combinations of arguments are supported:

        1. j_obj: JavaObject -> directly wrap the instance;
        2. no arguments: call DenseVector();
        3. n: int -> : call `DenseVector(n)` of Java side;
        4. data: List[Double] -> call `DenseVector(double[] data)`  of Java side;

        :param args: arguments, see function description.
        """
        if len(args) == 1 and isinstance(args[0], JavaObject):
            j_obj = args[0]
        else:
            j_obj = call_java_method(self._j_cls(), *args).get_j_obj()
        super(DenseVector, self).__init__(j_obj)

    def setData(self, data):
        return self.setData(data)

    def setEqual(self, other):
        return self.setEqual(other)

    def plusEqual(self, other):
        return self.plusEqual(other)

    def minusEqual(self, other):
        return self.minusEqual(other)

    def plusScaleEqual(self, other, alpha):
        return self.plusScaleEqual(other, alpha)

    @classmethod
    @auto_convert_java_type
    def zeros(cls, n):
        return cls._j_cls().zeros(n)

    @classmethod
    @auto_convert_java_type
    def ones(cls, n):
        return cls._j_cls().ones(n)

    @classmethod
    @auto_convert_java_type
    def rand(cls, n):
        return cls._j_cls().rand(n)

    def getData(self):
        return self.getData()

    def toSparseVector(self):
        return self.toSparseVector()

    def toDisplayData(self, n: int = None):
        if n is None:
            return self.toDisplayData()
        else:
            return self.toDisplayData(n)

    def toDisplaySummary(self) -> str:
        return self.toDisplaySummary()

    def toShortDisplayData(self) -> str:
        return self.toShortDisplayData()

    _unsupported_j_methods = ['toBytes']


class SparseVector(Vector, DataTypeDisplay):
    """
    SparseVector
    """
    _j_cls_name = 'com.alibaba.alink.common.linalg.SparseVector'

    def __init__(self, *args):
        """
        Construct `SparseVector` from arguments with a wrapped Java instance.
        Different combinations of arguments are supported:

        1. j_obj: JavaObject -> directly wrap the instance;
        2. no arguments -> call SparseVector();
        3. n: int -> call `SparseVector(n)` of Java side;
        4. n: int, indices: List[int], values: List[int] -> call `SparseVector(int n, int[] indices, double[] values)` of Java side

        :param args: arguments, see function description.
        """
        if len(args) == 1 and isinstance(args[0], JavaObject):
            j_obj = args[0]
        elif len(args) == 3:
            j_obj = call_java_method(self._j_cls(), args[0], list(args[1]), list(args[2])).get_j_obj()
        else:
            j_obj = call_java_method(self._j_cls(), *args).get_j_obj()
        super(SparseVector, self).__init__(j_obj)

    def forEach(self, action):
        for (index, value) in zip(self.getIndices(), self.getValues()):
            action(index, value)
        return None

    def setSize(self, n):
        return self.setSize(n)

    def getIndices(self):
        return self.getIndices()

    def getValues(self):
        return self.getValues()

    def numberOfValues(self):
        return self.numberOfValues()

    def removeZeroValues(self):
        return self.removeZeroValues()

    def toDenseVector(self):
        return self.toDenseVector()

    def toDisplayData(self, n: int = None):
        if n is None:
            return self.toDisplayData()
        else:
            return self.toDisplayData(n)

    def toDisplaySummary(self) -> str:
        return self.toDisplaySummary()

    def toShortDisplayData(self) -> str:
        return self.toShortDisplayData()

    _unsupported_j_methods = ['toBytes']


class DenseMatrix(JavaObjectWrapperWithAutoTypeConversion):
    """
    DenseMatrix
    """
    _j_cls_name = 'com.alibaba.alink.common.linalg.DenseMatrix'

    def __init__(self, *args):
        """
        Construct `DenseMatrix` from arguments with a wrapped Java instance.
        Different combinations of arguments are supported:

        1. j_obj: JavaObject -> directly wrap the instance;
        2. no arguments -> call DenseMatrix();
        3. m: int, n: int -> call `DenseMatrix(m, n)` of Java side;
        4. m: int, n: int, data: List[Double] -> call `DenseMatrix(m, n, data)` of Java side;
        5. m: int, n: int, data: List[Double], inRowMajor: bool -> call `DenseMatrix(m, n, data, inRowMajor)` of Java side;
        6. data: List[List[Double]] -> call `DenseMatrix(data)` of Java side.

        :param args: arguments, see function description.
        """
        if len(args) == 1 and isinstance(args[0], JavaObject):
            j_obj = args[0]
        else:
            j_obj = call_java_method(self._j_cls(), *args).get_j_obj()
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def add(self, i, j, s):
        return self.add(i, j, s)

    def get(self, i, j):
        return self.get(i, j)

    def clone(self):
        return self.clone()

    def set(self, i, j, s):
        return self.set(i, j, s)

    def sum(self):
        return self.sum()

    def scale(self, v):
        return self.scale(v)

    @classmethod
    @auto_convert_java_type
    def eye(cls, m, n=None):
        if n is None:
            return cls._j_cls().eye(m)
        else:
            return cls._j_cls().eye(m, n)

    @classmethod
    @auto_convert_java_type
    def zeros(cls, m, n):
        return cls._j_cls().zeros(m, n)

    @classmethod
    @auto_convert_java_type
    def ones(cls, m, n):
        return cls._j_cls().ones(m, n)

    @classmethod
    @auto_convert_java_type
    def rand(cls, m, n):
        return cls._j_cls().rand(m, n)

    @classmethod
    @auto_convert_java_type
    def randSymmetric(cls, n):
        return cls._j_cls().randSymmetric(n)

    def getArrayCopy2D(self):
        return self.getArrayCopy2D()

    def getArrayCopy1D(self, inRowMajor):
        return self.getArrayCopy1D(inRowMajor)

    def getRow(self, row):
        return self.getRow(row)

    def getColumn(self, col):
        return self.getColumn(col)

    def selectRows(self, rows):
        return self.selectRows(rows)

    def getSubMatrix(self, m0, m1, n0, n1):
        return self.getSubMatrix(m0, m1, n0, n1)

    def setSubMatrix(self, sub, m0, m1, n0, n1):
        return self.setSubMatrix(sub, m0, m1, n0, n1)

    def isSquare(self):
        return self.isSquare()

    def isSymmetric(self):
        return self.isSymmetric()

    def numRows(self):
        return self.numRows()

    def numCols(self):
        return self.numCols()

    def plusEquals(self, alpha_or_mat):
        return self.plusEquals(alpha_or_mat)

    def minusEquals(self, mat):
        return self.minusEquals(mat)

    def multiplies(self, vec_or_mat):
        return self.multiplies(vec_or_mat)

    def transpose(self):
        return self.transpose()

    def norm2(self):
        return self.norm2()

    def cond(self):
        return self.cond()

    def det(self):
        return self.det()

    def rank(self):
        return self.rank()

    def solve(self, vec_or_mat):
        return self.solve(vec_or_mat)

    def solveLS(self, vec_or_mat):
        return self.solveLS(vec_or_mat)

    def inverse(self):
        return self.inverse()

    def pseudoInverse(self):
        return self.pseudoInverse()

    def scaleEqual(self, v):
        return self.scaleEqual(v)

    def plus(self, alpha_or_mat):
        return self.plus(alpha_or_mat)

    def minus(self, mat):
        return self.minus(mat)

    def getData(self):
        return self.getData()


class VectorIterator(JavaObjectWrapperWithAutoTypeConversion):
    """
    VectorIterator
    """
    _j_cls_name = 'com.alibaba.alink.common.linalg.VectorIterator'

    def __init__(self, j_obj):
        self._j_obj = j_obj

    def get_j_obj(self):
        return self._j_obj

    def getValue(self):
        return self.getValue()

    def hasNext(self):
        return self.hasNext()

    def next(self):
        return self.next()

    def getIndex(self):
        return self.getIndex()
