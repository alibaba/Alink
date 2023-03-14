package com.alibaba.alink.operator.common.outlier.tsa.tsacalculator;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.common.linalg.DenseVector;
import com.alibaba.alink.operator.common.outlier.tsa.STLMethod;
import com.alibaba.alink.params.timeseries.HasFrequency;

public class STLDecomposerCalc extends TimeSeriesDecomposerCalc {
	private static final long serialVersionUID = 6666098921428864819L;
	private int frequency;

	public STLDecomposerCalc(Params params) {
		frequency = params.get(HasFrequency.FREQUENCY);
	}

	@Override
	public DenseVector[] decompose(double[] data) {
		return decompose(data, frequency);
	}

	public static DenseVector[] decompose(double[] data, int frequency) {
		STLMethod stl = new STLMethod(data, frequency, "periodic", 0, null, null,
			null, null, null, null, null, true, null, null);
		return new DenseVector[] {new DenseVector(stl.trend),
			new DenseVector(stl.seasonal), new DenseVector(stl.remainder)};
	}
}
