package com.alibaba.alink.common.linalg

import breeze.linalg.cholesky
import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import org.netlib.util.intW

/** Cholesky Decomposition.
  * For a symmetric, positive definite matrix A, the Cholesky decomposition
  * is an lower triangular matrix L so that A = L*L'.
  * Exception would be raised if the matrix is not symmetric positive definite
  * matrix.
  */
class CholeskyDecomposition(val A: DenseMatrix) {

    val c = cholesky(BreezeUtils.toBreezeMatrix(A))

    /**
      * Solve A*X = B. The result is returned in B.
      *
      * @param B A Matrix with as many rows as A and any number of columns.
      */
    def solve(B: DenseMatrix): Unit = {
        val n = A.numRows()
        val info = new intW(0)
        lapack.dpotrs("L", n, B.numCols(), c.data, n, B.getData, n, info)
        assert(info.`val` == 0, "A is not positive definite.")
    }

    /**
      * Get matrix L.
      */
    def getL(): DenseMatrix = {
        BreezeUtils.fromBreezeMatrix(c)
    }
}
