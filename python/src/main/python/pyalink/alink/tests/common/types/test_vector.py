import unittest

from pyalink.alink.common.types.vector import DenseVector, DenseMatrix, VectorIterator, SparseVector


class TestVector(unittest.TestCase):
    def test_dense_vector_init(self):
        dv0 = DenseVector()
        self.assertEqual(0, dv0.size())
        dv1 = DenseVector(10)
        self.assertEqual(10, dv1.size())
        lst = [1., 3., 5.]
        dv2 = DenseVector(lst)
        self.assertEqual(3, dv2.size())
        self.assertListEqual(lst, dv2.getData())

    def test_dense_vector(self):
        ones: DenseVector = DenseVector.ones(5)
        self.assertEqual(type(ones), DenseVector)
        for i in range(5):
            self.assertAlmostEquals(ones.get(i), 1., delta=1e-9)

        zeros: DenseVector = DenseVector.zeros(5)
        self.assertEqual(type(zeros), DenseVector)
        for i in range(5):
            self.assertAlmostEquals(zeros.get(i), 0., delta=1e-9)

        rand: DenseVector = DenseVector.rand(5)
        self.assertEqual(type(rand), DenseVector)
        self.assertEqual(rand.size(), 5)

        twos: DenseVector = ones.plus(ones)
        for i in range(5):
            self.assertAlmostEquals(twos.get(i), 2., delta=1e-9)

        outer_res0: DenseMatrix = twos.outer()
        self.assertAlmostEquals(outer_res0.numRows(), 5., delta=1e-9)
        self.assertAlmostEquals(outer_res0.numCols(), 5., delta=1e-9)

        dv: DenseVector = ones.clone()
        self.assertEqual(type(dv), DenseVector)
        dv.set(3, 4.)
        self.assertAlmostEquals(dv.get(3), 4., delta=1e-9)

        data = dv.getData()
        self.assertEqual(type(data), list)
        self.assertAlmostEquals(data[3], 4., delta=1e-9)

        data[1] = 3.
        dv.setData(data)
        self.assertAlmostEquals(dv.get(1), 3., delta=1e-9)

        dv_it = dv.iterator()
        self.assertEqual(type(dv_it), VectorIterator)
        while dv_it.hasNext():
            print(dv_it.getIndex(), ",", dv_it.getValue())
            dv_it.next()

        dv2 = dv.outer().solveLS(dv)
        self.assertEqual(type(dv2), DenseVector)
        print(dv2)

        sv = dv.toSparseVector()
        self.assertEqual(type(sv), SparseVector)
        print(sv)

        dv2 = dv.clone()
        self.assertEqual(type(dv2), DenseVector)
        print(dv2)

        print(dv.toDisplayData())
        print(dv.toDisplayData(10))
        print(dv.toDisplaySummary())
        print(dv.toShortDisplayData())

    def test_sparse_vector_init(self):
        sv0 = SparseVector()
        self.assertEqual(sv0.size(), -1)
        print(sv0.size())
        sv0 = SparseVector(5)
        self.assertEqual(sv0.size(), 5)
        data = dict({1: 0.1, 4: 0.4, 8: 0.8})
        sv1 = SparseVector(9, data.keys(), data.values())
        self.assertEqual(sv1.size(), 9)

    def test_sparse_vector(self):
        data = dict({1: 0.1, 4: 0.4, 8: 0.8})
        sv: SparseVector = SparseVector(9, data.keys(), data.values())

        sv = sv.prefix(1.0)
        self.assertEqual(sv.size(), 10)
        self.assertAlmostEquals(sv.get(0), 1.)

        sv = sv.append(2.0)
        self.assertEqual(sv.size(), 11)
        self.assertAlmostEquals(sv.get(10), 2.)

        self.assertEqual(sv.numberOfValues(), 5)

        sv2 = sv.clone()
        self.assertEqual(type(sv), SparseVector)
        print(sv2)

        sv_it: VectorIterator = sv.iterator()
        self.assertEqual(type(sv_it), VectorIterator)
        while sv_it.hasNext():
            print(sv_it.getIndex(), ",", sv_it.getValue())
            sv_it.next()

        dv: DenseVector = sv.toDenseVector()
        self.assertEqual(type(dv), DenseVector)
        self.assertEqual(dv.size(), 11)

        sv_with_zero: SparseVector = sv.append(0.)
        self.assertEqual(6, sv_with_zero.numberOfValues())
        sv_with_zero.removeZeroValues()
        self.assertEqual(5, sv_with_zero.numberOfValues())

        outer: DenseMatrix = sv.outer()
        self.assertEqual(outer.numRows(), sv.size())
        self.assertEqual(outer.numCols(), sv.size())

        sv.forEach(lambda k, v: print(k, "=", v))

        sv2 = sv.clone()
        self.assertEqual(type(sv2), SparseVector)
        print(sv2)

        print(sv.toDisplayData())
        print(sv.toDisplayData(10))
        print(sv.toDisplaySummary())
        print(sv.toShortDisplayData())

    def assert_matrix_size(self, mat, m, n):
        self.assertEqual(mat.numRows(), m)
        self.assertEqual(mat.numCols(), n)

    def test_dense_matrix_init(self):
        dm = DenseMatrix()
        self.assert_matrix_size(dm, 0, 0)

        dm = DenseMatrix(4, 5)
        self.assert_matrix_size(dm, 4, 5)

        dm = DenseMatrix(2, 3, [1., 2., 3., 4., 5., 6.])
        self.assert_matrix_size(dm, 2, 3)
        self.assertAlmostEquals(dm.get(0, 2), 5., delta=1e-9)

        dm = DenseMatrix(2, 3, [1., 2., 3., 4., 5., 6.], True)
        self.assert_matrix_size(dm, 2, 3)
        self.assertAlmostEquals(dm.get(0, 2), 3., delta=1e-9)

        dm = DenseMatrix([[1., 2.], [3., 4.], [5., 6.]])
        self.assert_matrix_size(dm, 3, 2)
        self.assertAlmostEquals(dm.get(1, 0), 3., delta=1e-9)

        dm = DenseMatrix.eye(3)
        self.assert_matrix_size(dm, 3, 3)

        dm = DenseMatrix.eye(3, 4)
        self.assert_matrix_size(dm, 3, 4)

        dm = DenseMatrix.randSymmetric(4)
        self.assert_matrix_size(dm, 4, 4)

    def test_dense_matrix(self):
        dm = DenseMatrix(2, 3, [1., 2., 3., 4., 5., 6.])
        self.assert_matrix_size(dm, 2, 3)
        self.assertAlmostEquals(dm.get(0, 2), 5., delta=1e-9)

        self.assertListEqual([1., 2., 3., 4., 5., 6.], dm.getData())
        self.assertListEqual([2., 4., 6.], dm.getRow(1))
        self.assertListEqual([3., 4.], dm.getColumn(1))

        self.assertListEqual([[1.0, 3.0, 5.0], [2.0, 4.0, 6.0]], dm.getArrayCopy2D())
        self.assertListEqual([1., 2., 3., 4., 5., 6.], dm.getArrayCopy1D(False))

        self.assertListEqual([[2.0, 4.0, 6.0]], dm.selectRows([1]).getArrayCopy2D())

        sub_mat: DenseMatrix = dm.getSubMatrix(0, 1, 2, 3)
        self.assertListEqual([[5.]], sub_mat.getArrayCopy2D())

        sub_mat.set(0, 0, 10.)
        dm.setSubMatrix(sub_mat, 0, 1, 2, 3)
        self.assertAlmostEquals(10., dm.get(0, 2), delta=1e-9)

        dm.add(0, 2, 2.)
        self.assertAlmostEquals(12., dm.get(0, 2), delta=1e-9)

        dm2 = dm.multiplies(dm.transpose())
        print(dm2)

        dm2 = dm.multiplies(SparseVector(3, [0, 2], [3., 5.]))
        print(dm2)

        print(dm.cond())
