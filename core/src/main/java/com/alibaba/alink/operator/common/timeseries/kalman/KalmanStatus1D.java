package com.alibaba.alink.operator.common.timeseries.kalman;

public class KalmanStatus1D {
	double P = 0.0;

	//Process Noise Cov
	double Q = 1e-5;

	//Measurement Noise Cov
	double R = 0.01;

	double x = 0.0;
	double K = 0.0;
}
