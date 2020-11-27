package com.alibaba.alink.operator.common.classification;

import com.alibaba.alink.common.linalg.BLAS;
import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.SparseVector;
import com.github.fommil.netlib.F2jBLAS;

public class NaiveBayesBLASUtil {
	/**
	 * For level-1 routines, we use Java implementation.
	 */
	private static final com.github.fommil.netlib.BLAS F2J_BLAS = new F2jBLAS();

	/**
	 * For level-2 and level-3 routines, we use the native BLAS.
	 * The NATIVE_BLAS instance tries to load BLAS implementations in the order:
	 * 1) optimized system libraries such as Intel MKL,
	 * 2) self-contained native builds using the reference Fortran from netlib.org,
	 * 3) F2J implementation.
	 * If to use optimized system libraries, it is important to turn of their multi-thread support.
	 * Otherwise, it will conflict with Flink's executor and leads to performance loss.
	 */
	private static final com.github.fommil.netlib.BLAS NATIVE_BLAS = com.github.fommil.netlib.BLAS.getInstance();

	/**
	 * y := alpha * A * x + beta * y .
	 */
	public static void gemv(double alpha, DenseMatrix matA,
							DenseVector x, double beta, DenseVector y) {
		final int m = matA.numRows();
		final int n = matA.numCols();
		final int lda = matA.numRows();
		final String ta = "N";
		NATIVE_BLAS.dgemv(ta, m, n, alpha, matA.getData(), lda, x.getData(), 1, beta, y.getData(), 1);
	}

	/**
	 * y := alpha * A * x + beta * y .
	 */
	public static void gemv(double alpha, DenseMatrix matA,
							SparseVector x, double beta, DenseVector y) {
		final int m = matA.numRows();

		BLAS.scal(beta, y);
		int[] indices = x.getIndices();
		double[] values = x.getValues();
		for (int i = 0; i < indices.length; i++) {
			int index = indices[i];
			if (index >= matA.numCols()) {
				break;
			}
			double value = alpha * values[i];
			F2J_BLAS.daxpy(m, value, matA.getData(), index * m, 1, y.getData(), 0, 1);
		}

	}
}
