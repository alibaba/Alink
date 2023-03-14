package com.alibaba.alink.operator.common.timeseries.kalman;

import java.util.Random;

public class KalmanFilter1D {

	private final KalmanStatus1D status;

	public KalmanFilter1D() {
		this(new KalmanStatus1D());
	}

	public KalmanFilter1D(KalmanStatus1D initStatus) {
		this.status = initStatus;
	}

	public double filter(double z) {
		status.P = status.P + status.Q;

		status.K = status.P / (status.P + status.R);

		status.x += status.K * (z - status.x);

		status.P -= status.K * status.P;

		return status.x;

	}

	public static void main(String[] args) throws Exception {
		Random rand = new Random(1234);
		KalmanFilter1D KF = new KalmanFilter1D();

		for (int i = 0; i < 50; i++) {
			double z = rand.nextGaussian();
			System.out.print(z);
			System.out.print("  \t");
			System.out.println(KF.filter(z));
		}
	}
}
