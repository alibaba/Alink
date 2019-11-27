package com.alibaba.alink.common.linalg

import breeze.linalg.{inv, DenseMatrix => BDM, DenseVector => BDV}

object BreezeUtils {

    private[linalg] def toBreezeMatrix(A: DenseMatrix): BDM[Double] = {
        new BDM[Double](A.numRows, A.numCols, A.getData, 0, A.numRows, false)
    }

    private[linalg] def toBreezeVector(v: DenseVector): BDV[Double] = {
        new BDV[Double](v.getData)
    }

    private[linalg] def fromBreezeMatrix(A: BDM[Double]): DenseMatrix = {
        require(A.offset == 0, "Offset of BDM not zero.")
        require(!A.isTranspose || A.majorStride == A.cols, "Major stride not equals to num columns")
        require(A.isTranspose || A.majorStride == A.rows, "Major stride not equals to num rows")
        if (A.isTranspose)
            new DenseMatrix(A.cols, A.rows, A.data).transpose()
        else
            new DenseMatrix(A.rows, A.cols, A.data)
    }

    private[linalg] def fromBreezeVector(v: BDV[Double]): DenseVector = {
        require(v.stride == 1)
        require(v.offset == 0)
        new DenseVector(v.data)
    }

    /**
      * Inverse matrix A.
      */
    def inverse(A: DenseMatrix): DenseMatrix = {
        val invA = inv(toBreezeMatrix(A))
        fromBreezeMatrix(invA)
    }

    /**
      * Return the determinant of A
      */
    def det(A: DenseMatrix): Double = {
        breeze.linalg.det(toBreezeMatrix(A))
    }

    /**
      * Return the rank of A
      */
    def rank(A: DenseMatrix): Int = {
        breeze.linalg.rank(toBreezeMatrix(A))
    }
}
