package com.alibaba.alink.pipeline.tuning;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.common.linalg.MatVecOp;

import java.util.List;

/**
 * https://blog.csdn.net/yangmingliang666/article/details/79645782
 */
public class GaussianProcessRegression {
	private final List <DenseVector> X;
	private final DenseVector y;
	private final int n;
	private final DenseMatrix matrixK;

	private static final double SIGMA_F_2 = 1.27 * 1.27;
	private static final double SIGMA_N_2 = 0.3 * 0.3;
	private static final double CONST_L = -0.5;

	public GaussianProcessRegression(List <DenseVector> X, List <Double> y) {
		if (null == X || null == y) {
			throw new IllegalArgumentException("Input CAN NOT be null!");
		}
		if (X.size() != y.size()) {
			throw new IllegalArgumentException("X and y MUST have the same size!");
		}
		this.X = X;
		this.n = this.X.size();
		this.y = new DenseVector(n);
		for (int i = 0; i < n; i++) {
			this.y.set(i, y.get(i));
		}

		this.matrixK = new DenseMatrix(n, n);
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < n; j++) {
				final double d = SIGMA_F_2 * Math.exp(MatVecOp.sumSquaredDiff(X.get(i), X.get(j)) * CONST_L);
				this.matrixK.set(i, j, d);
				this.matrixK.set(j, i, d);
			}
		}
		for (int i = 0; i < n; i++) {
			matrixK.set(i, i, SIGMA_F_2 + SIGMA_N_2);
		}
	}

	public Tuple2 <Double, Double> calc(DenseVector x) {
		DenseVector vectorK = new DenseVector(n);
		DenseMatrix matrixB = new DenseMatrix(n, 2);
		for (int i = 0; i < n; i++) {
			matrixB.set(i, 0, y.get(i));
		}
		for (int i = 0; i < n; i++) {
			final double d = SIGMA_F_2 * Math.exp(MatVecOp.sumSquaredDiff(X.get(i), x) * CONST_L);
			vectorK.set(i, d);
			matrixB.set(i, 1, d);
		}

		double kk = SIGMA_F_2 * Math.exp(MatVecOp.sumSquaredDiff(x, x) * CONST_L);

		DenseVector r = this.matrixK.solveLS(matrixB).transpose().multiplies(vectorK);

		return new Tuple2(r.get(0), kk - r.get(1));
	}

}
