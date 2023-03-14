package com.alibaba.alink.operator.common.outlier.tsa.tsacalculator;

import com.alibaba.alink.common.linalg.DenseVector;

import java.io.Serializable;

public abstract class TimeSeriesDecomposerCalc implements Serializable {
	private static final long serialVersionUID = 7014060875543471259L;

	public abstract DenseVector[] decompose(double[] data);

	public void reset() {}
}
