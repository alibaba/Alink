package com.alibaba.alink.operator.common.outlier.tsa.tsacalculator;

import java.io.Serializable;

public abstract class DecomposeOutlierDetectorCalc implements Serializable {
	private static final long serialVersionUID = -1356304484256542192L;

	public abstract int[] detect(double[] data);

	public void reset() {}
}
