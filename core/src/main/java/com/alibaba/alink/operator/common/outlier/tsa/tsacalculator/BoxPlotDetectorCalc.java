package com.alibaba.alink.operator.common.outlier.tsa.tsacalculator;

import org.apache.flink.ml.api.misc.param.Params;

import com.alibaba.alink.operator.common.outlier.TimeSeriesAnomsUtils;
import com.alibaba.alink.params.outlier.HasBoxPlotK;
import com.alibaba.alink.params.outlier.tsa.HasBoxPlotRoundMode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BoxPlotDetectorCalc extends DecomposeOutlierDetectorCalc {
	private static final long serialVersionUID = 2367292760089913511L;
	private HasBoxPlotRoundMode.RoundMode roundMode;
	private double k;

	public BoxPlotDetectorCalc(Params params) {
		roundMode = params.get(HasBoxPlotRoundMode.ROUND_MODE);
		k = params.get(HasBoxPlotK.K);
	}

	@Override
	public int[] detect(double[] data) {
		return calcBoxPlot(data, roundMode, k, false);
	}

	/**
	 * Here we consider the fact that data may contains NaN.
	 * If detectLast, we detect the last data which is not NaN.
	 */
	public static int[] calcBoxPlot(double[] data, HasBoxPlotRoundMode.RoundMode roundMode, double k,
									boolean detectLast) {
		int length = data.length;
		List <Double> listData = new ArrayList <>(length);
		for (double v : data) {
			if (Double.isNaN(v)) {
				length -= 1;
				continue;
			}
			listData.add(v);
		}

		if (detectLast && length <= 4) {
			throw new RuntimeException("in detectLast mode, the data size must be larger than 4.");
		}
		if (length <= 3) {
			return new int[0];
		}
		double[] sortedData;
		double lastData = 0;
		if (detectLast) {
			length -= 1;
			lastData = listData.get(length);
		}
		sortedData = new double[length];
		for (int i = 0; i < length; i++) {
			sortedData[i] = listData.get(i);
		}
		Arrays.sort(sortedData);
		double q1;
		double q3;
		switch (roundMode) {
			case CEIL:
				q1 = sortedData[(int) Math.ceil(length * 0.25)];
				q3 = sortedData[(int) Math.ceil(length * 0.75)];
				break;
			case FLOOR:
				q1 = sortedData[(int) Math.floor(length * 0.25)];
				q3 = sortedData[(int) Math.floor(length * 0.75)];
				break;
			case AVERAGE:
				q1 = (sortedData[(int) Math.ceil(length * 0.25)] + sortedData[(int) Math.floor(length * 0.25)]) / 2;
				q3 = (sortedData[(int) Math.ceil(length * 0.75)] + sortedData[(int) Math.floor(length * 0.75)]) / 2;
				break;
			default:
				throw new RuntimeException("Only support ceil, floor and average strategy.");
		}
		List <Integer> indices = new ArrayList <>();
		if (detectLast) {
			if (TimeSeriesAnomsUtils.judgeBoxPlotAnom(lastData, q1, q3, k)) {
				indices.add(length);
			}
		} else {
			for (int i = 0; i < length; i++) {
				if (TimeSeriesAnomsUtils.judgeBoxPlotAnom(data[i], q1, q3, k)) {
					indices.add(i);
				}
			}
		}
		return indices.stream().mapToInt(a -> a).toArray();
	}
}
