import unittest

import numpy as np
import pandas as pd
from py4j.java_gateway import JavaClass

from pyalink.alink import *
from pyalink.alink.common.types.tensor import DataType
from pyalink.alink.py4j_util import get_java_class


class TestTensor(unittest.TestCase):

    def test_tensor_methods_coverage(self):
        py_class = Tensor
        j_cls_name = py_class._j_cls_name
        py_func_set = set(filter(
            lambda d: not d.startswith("_") and d != "get_j_obj" and d != "j_cls_name",
            dir(py_class)))
        py_func_set -= {'toNdarray', 'fromNdarray'}
        if hasattr(py_class, '_unsupported_j_methods'):
            py_func_set = py_func_set.union(set(py_class._unsupported_j_methods))

        j_check_wrapper_util_cls = get_java_class("com.alibaba.alink.python.utils.CheckWrapperUtil")
        j_func_set = set(j_check_wrapper_util_cls.getJMethodNames(j_cls_name))
        self.assertSetEqual(py_func_set, j_func_set)

    def test_tensor_classes_existence(self):
        tensor_classes = Tensor.__subclasses__()
        for tensor_cls in tensor_classes:
            j_cls = tensor_cls._j_cls()
            self.assertTrue(isinstance(j_cls, JavaClass))

    def test_tensor_init(self):
        arr = np.array([[1, 2], [3, 4]], dtype=np.int32)
        tensor = Tensor.fromNdarray(arr)
        self.assertTrue(isinstance(tensor, Tensor))
        self.assertEqual(DataType.INT, tensor.getType())

        arr2 = tensor.toNdarray()
        self.assertTrue(isinstance(arr2, np.ndarray))
        self.assertEqual(arr.dtype, arr2.dtype)
        self.assertEqual(arr.shape, arr2.shape)

    def test_tensor(self):
        arr = np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype=np.int32)
        tensor = Tensor.fromNdarray(arr)

        # shape, size
        self.assertListEqual(tensor.shape(), list(arr.shape))
        self.assertEqual(tensor.size(), arr.size)

        # reshape, flatten
        tensor2 = tensor.reshape((2, 4))
        self.assertTrue(isinstance(tensor2, Tensor))
        self.assertListEqual(tensor2.shape(), [2, 4])

        tensor2 = tensor.flatten()
        self.assertTrue(isinstance(tensor2, Tensor))
        self.assertListEqual(tensor2.shape(), [8])

        tensor2 = tensor.flatten(0, 1)
        self.assertTrue(isinstance(tensor2, Tensor))
        self.assertListEqual(tensor2.shape(), [4, 2])

        # stack, unstack
        tensor2 = Tensor.fromNdarray(arr)
        stacked = Tensor.stack([tensor, tensor2], 2)
        self.assertTrue(isinstance(stacked, Tensor))
        self.assertListEqual(stacked.shape(), [2, 2, 2, 2])
        print(stacked)

        [tensor2, tensor3] = Tensor.unstack(stacked, 2)
        self.assertTrue(isinstance(tensor2, Tensor))
        self.assertTrue(isinstance(tensor3, Tensor))
        print(tensor2, tensor3)

        # getObject, setObject
        tensor2 = Tensor.fromNdarray(arr)
        self.assertTrue(isinstance(tensor2, Tensor))
        self.assertEqual(3, tensor2.getObject((0, 1, 0)))

        tensor2.setObject(33, (0, 1, 0))
        self.assertEqual(33, tensor2.getObject((0, 1, 0)))

    def test_numerical_tensor(self):
        arr = np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype=np.float32)
        tensor: FloatTensor = Tensor.fromNdarray(arr)

        res: FloatTensor = tensor.min(1, True)
        np.testing.assert_array_equal(arr.min(axis=1, keepdims=True), res.toNdarray())

        res: FloatTensor = tensor.max(1, False)
        np.testing.assert_array_equal(arr.max(axis=1, keepdims=False), res.toNdarray())

        res: FloatTensor = tensor.sum(1, True)
        np.testing.assert_array_equal(arr.sum(axis=1, keepdims=True), res.toNdarray())

        res: FloatTensor = tensor.mean(1, False)
        np.testing.assert_array_equal(arr.mean(axis=1, keepdims=False), res.toNdarray())

    def test_get_set(self):
        arr = np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype=np.float32)
        tensor: FloatTensor = Tensor.fromNdarray(arr)
        self.assertTrue(isinstance(tensor, (FloatTensor,)))
        np.testing.assert_array_equal(tensor.toNdarray(), arr)
        self.assertEqual(3., tensor.getFloat((0, 1, 0)))
        tensor.setFloat(33., (0, 1, 0))
        self.assertEqual(33., tensor.getFloat((0, 1, 0)))

        arr = np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype=np.float64)
        tensor: DoubleTensor = Tensor.fromNdarray(arr)
        self.assertTrue(isinstance(tensor, (DoubleTensor,)))
        np.testing.assert_array_equal(tensor.toNdarray(), arr)
        self.assertEqual(3., tensor.getDouble((0, 1, 0)))
        tensor.setDouble(33., (0, 1, 0))
        self.assertEqual(33., tensor.getDouble((0, 1, 0)))

        arr = np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype=np.int32)
        tensor: IntTensor = Tensor.fromNdarray(arr)
        self.assertTrue(isinstance(tensor, (IntTensor,)))
        np.testing.assert_array_equal(tensor.toNdarray(), arr)
        self.assertEqual(3, tensor.getInt((0, 1, 0)))
        tensor.setInt(33, (0, 1, 0))
        self.assertEqual(33, tensor.getInt((0, 1, 0)))

        arr = np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype=np.int64)
        tensor: LongTensor = Tensor.fromNdarray(arr)
        self.assertTrue(isinstance(tensor, (LongTensor,)))
        np.testing.assert_array_equal(tensor.toNdarray(), arr)
        self.assertEqual(3, tensor.getLong((0, 1, 0)))
        tensor.setLong(33, (0, 1, 0))
        self.assertEqual(33, tensor.getLong((0, 1, 0)))

        arr = np.array([[[True, False, True], [True, False, True]], [[False, True, False], [False, True, False]]],
                       dtype=np.bool)
        tensor: BoolTensor = Tensor.fromNdarray(arr)
        self.assertTrue(isinstance(tensor, (BoolTensor,)))
        np.testing.assert_array_equal(tensor.toNdarray(), arr)
        self.assertEqual(True, tensor.getBoolean((0, 1, 0)))
        tensor.setBoolean(False, (0, 1, 0))
        self.assertEqual(False, tensor.getBoolean((0, 1, 0)))

        arr = np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype='<b')
        tensor: ByteTensor = Tensor.fromNdarray(arr)
        self.assertTrue(isinstance(tensor, (ByteTensor,)))
        np.testing.assert_array_equal(tensor.toNdarray(), arr)
        self.assertEqual(3, tensor.getByte((0, 1, 0)))
        tensor.setByte(33, (0, 1, 0))
        self.assertEqual(33, tensor.getByte((0, 1, 0)))

        arr = np.array([[[1, 2], [3, 4]], [[5, 6], [7, 8]]], dtype='<B')
        tensor: UByteTensor = Tensor.fromNdarray(arr)
        self.assertTrue(isinstance(tensor, (UByteTensor,)))
        np.testing.assert_array_equal(tensor.toNdarray(), arr)
        self.assertEqual(3, tensor.getUByte((0, 1, 0)))
        tensor.setUByte(33, (0, 1, 0))
        self.assertEqual(33, tensor.getUByte((0, 1, 0)))

        arr = np.array([[['1', '2Hello'], ['3', '4World']], [['5', '6Java'], ['7Python', '8']]], dtype=np.str_)
        tensor: StringTensor = Tensor.fromNdarray(arr)
        self.assertTrue(isinstance(tensor, (StringTensor,)))
        np.testing.assert_array_equal(tensor.toNdarray(), arr)
        self.assertEqual('3', tensor.getString((0, 1, 0)))
        tensor.setString('33', (0, 1, 0))
        self.assertEqual('33', tensor.getString((0, 1, 0)))

    def test_tensor_collect_print(self):
        df = pd.DataFrame(["FLOAT#6#0.0 0.1 1.0 1.1 2.0 2.1 "])
        source = BatchOperator.fromDataframe(df, schemaStr='vec string')
        to_tensor = ToTensorBatchOp() \
            .setSelectedCol("vec") \
            .setTensorShape([2, 3]) \
            .setTensorDataType("float") \
            .linkFrom(source)
        res = to_tensor.collectToDataframe()
        print(res)  # in normal way
        self.assertTrue(isinstance(res.iloc[0].vec, Tensor))
        to_tensor.print()  # in multi-lines way
