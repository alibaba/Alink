package com.alibaba.alink.common.linalg

import breeze.linalg.svd

/** Singular Value Decomposition.
  * <P>
  * For an m-by-n matrix A with m >= n, the singular value decomposition is
  * an m-by-n orthogonal matrix U, an n-by-n diagonal matrix S, and
  * an n-by-n orthogonal matrix V so that A = U*S*V'.
  * <P>
  * The singular values, sigma[k] = S[k][k], are ordered so that
  * sigma[0] >= sigma[1] >= ... >= sigma[n-1].
  * <P>
  * The singular value decompostion always exists, so the constructor will
  * never fail.  The matrix condition number and the effective numerical
  * rank can be computed from this decomposition.
  */
class SingularValueDecomposition(val A: DenseMatrix) {

    val svd.SVD(u, s, vt) = svd(BreezeUtils.toBreezeMatrix(A))

    /**
      * Return the left singular vectors.
      */
    def getU(): DenseMatrix = {
        BreezeUtils.fromBreezeMatrix(u)
    }

    /**
      * Return the right singular vectors.
      */
    def getV(): DenseMatrix = {
        BreezeUtils.fromBreezeMatrix(vt).transpose()
    }

    /**
      * Return all sigular values.
      */
    def getSingularValues(): DenseVector = {
        BreezeUtils.fromBreezeVector(s)
    }

    /** Two norm condition number
      *
      * @return max(S)/min(S)
      */
    def cond(): Double = {
        val m = A.numRows()
        val n = A.numCols()
        s(0) / s(Math.min(m, n) - 1)
    }

    /** Effective numerical matrix rank
      *
      * @return Number of nonnegligible singular values.
      */
    def rank(): Int = {
        val m = A.numRows()
        val n = A.numCols()
        val eps = Math.pow(2.0, -52.0)
        val tol = Math.max(m, n) * s(0) * eps
        var r: Int = 0
        for (i <- 0 until s.length) {
            if (s(i) > tol)
                r = r + 1
        }
        r
    }

    /**
      * Two norm of A.
      *
      * @return
      */
    def norm2(): Double = {
        s(0)
    }
}
