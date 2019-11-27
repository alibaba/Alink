package com.alibaba.alink.common.linalg

import breeze.optimize.linear.NNLS
import com.alibaba.alink.common.linalg.BreezeUtils.{fromBreezeVector, toBreezeMatrix, toBreezeVector}

/**
  * Solver for least square problem with nonnegativity constraints.
  */
object NNLSSolver {
    /**
      * Solve a least squares problem, possibly with nonnegativity constraints, by a modified
      * projected gradient method.  That is, find x minimising ||Ax - b||_2 given A^T A and A^T b,
      * subject to x >= 0.
      */
    def solve(ata: DenseMatrix, atb: DenseVector): DenseVector = {
        val m = ata.numRows()
        val n = ata.numCols()
        require(m == n, "m != n")

        val nnls = new NNLS()
        val ret = nnls.minimize(toBreezeMatrix(ata), toBreezeVector(atb))
        fromBreezeVector(ret)
    }
}
