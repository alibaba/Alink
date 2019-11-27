package com.alibaba.alink.common.linalg

import breeze.linalg.LU
import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import org.netlib.util.intW

/** Computes the LU factorization of the given real M-by-N matrix A such that
  * A = P * L * U where P is a permutation matrix (row exchanges).
  * Upon completion, a tuple consisting of a matrix 'lu' and an integer array 'p'.
  * The upper triangular portion of 'lu' resembles U whereas the lower triangular portion of
  * 'lu' resembles L up to but not including the diagonal elements of L which are
  * all equal to 1.
  * The primary use of the LU decomposition is in the solution of square systems
  * of simultaneous linear equations. This will fail if isNonsingular() returns false.
  */
class LUDecomposition(val A: DenseMatrix) {

    val (lu, p) = LU(BreezeUtils.toBreezeMatrix(A))

    /**
      * Check whether matrix A is singular
      *
      * @return
      */
    def isNonsingular: Boolean = {
        var j = 0
        val n = lu.cols
        while (j < n) {
            if (lu(j, j) == 0.0)
                return false
            j += 1
        }
        true
    }

    /**
      * Solve A*X = B. The result is returned in B.
      *
      * @param B A Matrix with as many rows as A and any number of columns.
      */
    def solve(B: DenseMatrix): Unit = {
        val n = A.numRows()
        val info = new intW(0)
        lapack.dgetrs("N", n, B.numCols(), lu.data, n, p, B.getData, n, info)
        assert(info.`val` == 0, "A is singular")
    }

    /**
      * Get L and U, packed in a single matrix.
      * The upper triangular portion of LU resembles U whereas the lower triangular portion of
      * LU resembles L up to but not including the diagonal elements of L which are
      * all equal to 1.
      */
    def getLU: DenseMatrix = {
        BreezeUtils.fromBreezeMatrix(lu)
    }

    /**
      * Get permutation array P
      */
    def getP: Array[Int] = {
        p
    }
}
