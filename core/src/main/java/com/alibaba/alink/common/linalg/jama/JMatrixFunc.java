/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.alibaba.alink.common.linalg.jama;

import com.alibaba.alink.common.linalg.DenseMatrix;

/**
 * @author yangxu
 */
public class JMatrixFunc {

	public static DenseMatrix copy(DenseMatrix jm) {
		int m = jm.numRows();
		int n = jm.numCols();
		double[][] A = jm.getArrayCopy2D();
		DenseMatrix X = new DenseMatrix(m, n);
		double[][] C = X.getArrayCopy2D();
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				C[i][j] = A[i][j];
			}
		}
		return X;
	}

	public static DenseMatrix transpose(DenseMatrix jm) {
		int m = jm.numRows();
		int n = jm.numCols();
		double[][] A = jm.getArrayCopy2D();
		DenseMatrix X = new DenseMatrix(n, m);
		double[][] C = X.getArrayCopy2D();
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				C[j][i] = A[i][j];
			}
		}
		return X;
	}

	public static double norm1(DenseMatrix jm) {
		return new JaMatrix(jm).norm1();
	}

	public static double norm2(DenseMatrix jm) {
		return new JaMatrix(jm).norm2();
	}

	public static double normInf(DenseMatrix jm) {
		return new JaMatrix(jm).normInf();
	}

	public static double normF(DenseMatrix jm) {
		return new JaMatrix(jm).normF();
	}

	public static DenseMatrix uminus(DenseMatrix jm) {
		return new JaMatrix(jm).uminus().toJMatrix();
	}

	public static DenseMatrix add(DenseMatrix jm1, DenseMatrix jm2) {
		return new JaMatrix(jm1).plus(new JaMatrix(jm2)).toJMatrix();
	}

	public static DenseMatrix subtract(DenseMatrix jm1, DenseMatrix jm2) {
		return new JaMatrix(jm1).minus(new JaMatrix(jm2)).toJMatrix();
	}

	public static DenseMatrix multiply(DenseMatrix jm, double d) {
		return new JaMatrix(jm).times(d).toJMatrix();
	}

	public static DenseMatrix multiply(DenseMatrix jm, DenseMatrix jm1) {
		return new JaMatrix(jm).times(new JaMatrix(jm1)).toJMatrix();
	}

	public static DenseMatrix[] lu(DenseMatrix jm) {
		DenseMatrix[] jms = new DenseMatrix[3];
		LUDecomposition lud = new JaMatrix(jm).lu();
		jms[0] = lud.getL().toJMatrix();
		jms[1] = new JaMatrix(lud.getDoublePivot(), lud.getPivot().length).toJMatrix();
		jms[2] = lud.getU().toJMatrix();
		return jms;
	}

	public static DenseMatrix[] qr(DenseMatrix jm) {
		QRDecomposition qr = new JaMatrix(jm).qr();
		DenseMatrix[] jms = new DenseMatrix[2];
		jms[0] = qr.getQ().toJMatrix();
		jms[1] = qr.getR().toJMatrix();
		return jms;
	}

	public static DenseMatrix chol(DenseMatrix jm) {
		CholeskyDecomposition chol = new JaMatrix(jm).chol();
		return chol.getL().toJMatrix();
	}

	public static DenseMatrix[] svd(DenseMatrix jm) {
		if (jm.numRows() < jm.numCols()) {
			SingularValueDecomposition svd = new JaMatrix(jm.transpose()).svd();
			DenseMatrix[] jms = new DenseMatrix[3];
			jms[0] = svd.getV().toJMatrix();
			jms[1] = svd.getS().toJMatrix();
			jms[2] = svd.getU().toJMatrix();
			return jms;
		} else {
			SingularValueDecomposition svd = new JaMatrix(jm).svd();
			DenseMatrix[] jms = new DenseMatrix[3];
			jms[0] = svd.getU().toJMatrix();
			jms[1] = svd.getS().toJMatrix();
			jms[2] = svd.getV().toJMatrix();
			return jms;
		}
	}

	public static DenseMatrix[] eig(DenseMatrix jm) {
		EigenvalueDecomposition ed = new JaMatrix(jm).eig();
		DenseMatrix[] jms = new DenseMatrix[2];
		jms[0] = ed.getV().toJMatrix();
		jms[1] = ed.getD().toJMatrix();
		return jms;
	}

	public static DenseMatrix solve(DenseMatrix A, DenseMatrix B) {
		return new JaMatrix(A).solve(new JaMatrix(B)).toJMatrix();
	}

	public static DenseMatrix solveTranspose(DenseMatrix A, DenseMatrix B) {
		return new JaMatrix(A).solveTranspose(new JaMatrix(B)).toJMatrix();
	}

	public static DenseMatrix inverse(DenseMatrix jm) {
		return new JaMatrix(jm).inverse().toJMatrix();
	}

	public static double det(DenseMatrix jm) {
		double[][] Btmp = jm.getArrayCopy2D();
		boolean isZeroMatrix = true;
		for (int i = 0; i < jm.numRows(); i++) {
			for (int j = 0; j < jm.numCols(); j++) {
				if (Btmp[i][j] != 0) {
					isZeroMatrix = false;
				}
			}
		}
		if (isZeroMatrix) {
			return 0;
		}
		return new JaMatrix(jm).det();
	}

	public static double detLog(DenseMatrix jm) {
		double[][] Btmp = jm.getArrayCopy2D();
		boolean isZeroMatrix = true;
		for (int i = 0; i < jm.numRows(); i++) {
			for (int j = 0; j < jm.numCols(); j++) {
				if (Btmp[i][j] != 0) {
					isZeroMatrix = false;
				}
			}
		}
		if (isZeroMatrix) {
			return Double.MIN_VALUE;
		}
		return new JaMatrix(jm).detLog();
	}

	public static int rank(DenseMatrix jm) {
		return new JaMatrix(jm).rank();
	}

	public static double cond(DenseMatrix jm) {
		return new JaMatrix(jm).cond();
	}

	public static double trace(DenseMatrix jm) {
		return new JaMatrix(jm).trace();
	}

	public static DenseMatrix random(int m, int n) {
		return JaMatrix.random(m, n).toJMatrix();
	}

	public static DenseMatrix identity(int m, int n) {
		return JaMatrix.identity(m, n).toJMatrix();
	}

	public static DenseMatrix solveLS(DenseMatrix jm, DenseMatrix B) {
		return new JaMatrix(jm).solveLS(new JaMatrix(B)).toJMatrix();
	}

	public static void print(DenseMatrix jm, int w, int v) {
		new JaMatrix(jm).print(w, v);
	}

	/**
	 * Is the matrix nonsingular?
	 *
	 * @return true if U, and hence A, is nonsingular.
	 */
	public static boolean isNonsingular(DenseMatrix jm) {
		return new LUDecomposition(new JaMatrix(jm)).isNonsingular();
	}
}
