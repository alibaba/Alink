package com.alibaba.alink.operator.common.outlier.tsa.tsacalculator;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.outlier.TimeSeriesAnomsUtils;
import com.alibaba.alink.params.outlier.HasKSigmaK;

import java.util.ArrayList;
import java.util.List;

public class KSigmaDetectorCalc extends DecomposeOutlierDetectorCalc {

	private double k;

	public KSigmaDetectorCalc(Params params) {
		this.k = params.get(HasKSigmaK.K);
	}

	@Override
	public int[] detect(double[] data) {
		return calcKSigma(data, this.k, false);
	}

	//calc ksigma in the whole time serial.
	public static int[] calcKSigma(double[] data, double k, boolean detectLast) {
		double sum = 0.0;
		double squareSum = 0.0;
		int length = data.length;

		if (detectLast) {
			for (int i = 0; i < data.length - 1; i++) {
				sum += data[i];
				squareSum += Math.pow(data[i], 2);
			}
		} else {
			for (double datum : data) {
				if (Double.isNaN(datum)) {
					length -= 1;
					continue;
				}
				sum += datum;
				squareSum += Math.pow(datum, 2);
			}
		}
		double mean = sum / length;
		double variance;
		if (length == 0 || length == 1) {
			variance = 0;
		} else {
			variance = Math.max((squareSum - Math.pow(sum, 2) / length) / (length - 1), 0);
		}
		List <Integer> indices = new ArrayList <>();
		if (detectLast) {
			int i = length - 1;
			double score = TimeSeriesAnomsUtils.calcKSigmaScore(data[i], mean, variance);
			if (score >= k) {
				indices.add(i);
			}
		} else {
			for (int i = 0; i < length; i++) {
				double score = TimeSeriesAnomsUtils.calcKSigmaScore(data[i], mean, variance);
				if (score >= k) {
					indices.add(i);
				}
			}
		}

		return indices.stream().mapToInt(a -> a).toArray();
	}
}
