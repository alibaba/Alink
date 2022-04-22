package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.probabilistic.CDF;
import com.alibaba.alink.params.outlier.DynamicTimeWarpingDetectorParams;

import java.util.HashMap;
import java.util.Map;

/**
 * only support detect last.
 */
public class DynamicTimeWarpingDetector extends OutlierDetector {

	final private String selectedCol;
	final private int searchWindow;
	final private int period;
	final private int seriesLength;
	final private int historicalSeriesNum;

	public DynamicTimeWarpingDetector(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.selectedCol = params.get(DynamicTimeWarpingDetectorParams.FEATURE_COL);
		this.period = params.get(DynamicTimeWarpingDetectorParams.PERIOD);
		this.seriesLength = params.get(DynamicTimeWarpingDetectorParams.SERIES_LENGTH);
		this.searchWindow = params.get(DynamicTimeWarpingDetectorParams.SEARCH_WINDOW);
		this.historicalSeriesNum = params.get(DynamicTimeWarpingDetectorParams.HISTORICAL_SERIES_NUM);
	}

	@Override
	public Tuple3 <Boolean, Double, Map <String, String>>[] detect(MTable series, boolean detectLast) throws Exception {
		double[] data = OutlierUtil.getNumericArray(series, selectedCol);
		int n = data.length;
		int periodNum = data.length / this.period;
		int historicalSeriesNum = Math.min(periodNum - 1, this.historicalSeriesNum);
		if (historicalSeriesNum < 3) {
			Tuple3 <Boolean, Double, Map <String, String>>[] results = new Tuple3[1];
			HashMap <String, String> infoMap = new HashMap <>();
			infoMap.put("errorInfo", "series data number must >= 4 * period");
			results[0] = Tuple3.of(false, null, infoMap);
			return results;
		}

		double[] d1 = new double[historicalSeriesNum];
		int historicalStart = n - period;
		for (int i = 0; i < historicalSeriesNum; i++) {
			d1[i] = dtw(data, n - this.seriesLength, n,
				Math.max(historicalStart - this.seriesLength, 0), historicalStart, this.searchWindow);
			historicalStart -= period;
		}

		double[] d2 = new double[historicalSeriesNum * (historicalSeriesNum - 1) / 2];
		int idx = 0;
		for (int i = 0; i < historicalSeriesNum; i++) {
			for (int j = i + 1; j < historicalSeriesNum; j++) {
				d2[idx++] = dtw(data,
					n - (i + 1) * period - this.seriesLength, n - (i + 1) * period,
					Math.max(n - (j + 1) * period - this.seriesLength, 0), n - (j + 1) * period,
					this.searchWindow);
			}
		}

		double mean1 = mean(d1);
		double mean2 = mean(d2);
		int n1 = d1.length;
		int n2 = d2.length;
		double var1 = var(d1, mean1);
		double var2 = var(d2, mean2);

		double t = 0;
		if (var1 != 0 || var2 != 0) {
			t = (mean1 - mean2) / Math.sqrt(
				((n1 - 1) * var1 + (n2 - 1) * var2) / (n1 + n2 - 2) * (1.0 / n1 + 1.0 / n2));
		}

		double p = 2.0 * (1.0 - CDF.studentT(Math.abs(t), n1 + n2 - 2));

		double alpha = 0.01;

		Tuple3 <Boolean, Double, Map <String, String>>[] results = new Tuple3[1];
		HashMap <String, String> infoMap = new HashMap <>();
		infoMap.put("alpha", String.valueOf(0.01));
		infoMap.put("p", String.valueOf(p));
		infoMap.put("t", String.valueOf(t));
		results[0] = Tuple3.of(p < alpha, p, infoMap);

		return results;
	}

	static double mean(double[] data) {
		double s = 0;
		for (double val : data) {
			s += val;
		}
		return s / data.length;
	}

	static double var(double[] data, double mean) {
		double s = 0;
		for (double val : data) {
			s += (val - mean) * (val - mean);
		}
		return s == 0 ? 0.00001 : s / (data.length - 1);
	}

	static double dtw(double[] series, int start1, int end1, int start2, int end2, int searchWindow) {
		int n = end1 - start1;
		int m = end2 - start2;
		int w = Math.min(searchWindow, Math.abs(n - m));

		double[][] dtwMatrix = new double[n + 1][m + 1];
		for (int i = 0; i < n + 1; i++) {
			for (int j = 0; j < m + 1; j++) {
				dtwMatrix[i][j] = Double.POSITIVE_INFINITY;
			}
		}
		dtwMatrix[0][0] = 0;

		for (int i = 1; i < n + 1; i++) {
			for (int j = Math.max(1, i - w); j < Math.min(i + w, m) + 1; j++) {
				double cost = Math.abs(series[start1 + i - 1] - series[start2 + j - 1]);

				double last_min = Math.min(
					Math.min(
						dtwMatrix[i][j - 1],
						dtwMatrix[i - 1][j]
					),
					dtwMatrix[i - 1][j - 1]);
				dtwMatrix[i][j] = last_min + cost;
			}
		}
		return dtwMatrix[n][m];
	}

	static double dtw(double[] series1, double[] series2, int searchWindow) {
		double[] series = new double[series1.length + series2.length];
		System.arraycopy(series1, 0, series, 0, series1.length);
		System.arraycopy(series2, 0, series, series1.length, series2.length);
		return dtw(series, 0, series1.length, series1.length, series.length, searchWindow);
	}
}
