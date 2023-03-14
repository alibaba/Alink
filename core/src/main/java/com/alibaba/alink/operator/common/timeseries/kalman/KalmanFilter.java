package com.alibaba.alink.operator.common.timeseries.kalman;

import com.alibaba.alink.common.linalg.DenseVector;

import java.util.Random;

/**
 * http://www.cs.unc.edu/~welch/media/pdf/kalman_intro.pdf
 */
public class KalmanFilter {

	private final KalmanStatus status;

	public KalmanFilter() {
		this(new KalmanStatus(2));
	}

	public KalmanFilter(KalmanStatus initStatus) {
		this.status = initStatus;
	}

	public DenseVector filter(DenseVector z) {
		status.P.plusEquals(status.Q);

		status.K = status.P.plus(status.R).solveLS(status.P);

		status.x.plusEqual(status.K.multiplies(z.minus(status.x)));

		status.P.minusEquals(status.K.multiplies(status.P));

		return status.x;

	}

	public static void main(String[] args) throws Exception {
		Random rand = new Random(1234);
		KalmanFilter KF = new KalmanFilter();

		for (int i = 0; i < 50; i++) {
			DenseVector z = new DenseVector(new double[] {rand.nextGaussian(), rand.nextGaussian()});
			System.out.print(z);
			System.out.print("  \t");
			System.out.println(KF.filter(z));
		}
	}
}
