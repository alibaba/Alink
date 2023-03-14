package com.alibaba.alink.operator.common.timeseries.kalman;

import com.alibaba.alink.common.linalg.DenseMatrix;
import com.alibaba.alink.common.linalg.DenseVector;

public class KalmanStatus {
	DenseMatrix P;

	//Process Noise Cov
	DenseMatrix Q;

	//Measurement Noise Cov
	DenseMatrix R;

	DenseVector x;
	DenseMatrix K;

	final int dim;

	public KalmanStatus(int dim) {
		this.dim = dim;

		P = DenseMatrix.zeros(this.dim, this.dim);

		Q = DenseMatrix.eye(this.dim).scale(1e-5);

		R = DenseMatrix.eye(this.dim).scale(0.01);

		x = new DenseVector(this.dim);

		K = DenseMatrix.zeros(this.dim, this.dim);
	}

}
