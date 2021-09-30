package com.alibaba.alink.operator.common.timeseries.sarima;

import java.util.ArrayList;

public abstract class SarimaEstimate {

	public double[] sARCoef;
	public double[] sMACoef;
	public double[] sArStdError;
	public double[] sMaStdError;
	public double[] sResidual;
	public double logLikelihood;
	public double variance;


	public ArrayList <String> warn;

	public abstract void compute(double[][] seasonMatrix, int p, int q, int seasonalPeriod);

}
