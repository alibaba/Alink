package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.params.outlier.HasDirection.Direction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * box plot detector.
 */
public class BoxPlotDetector extends OutlierDetector {

	private final double K;
	private final String selectedCol;
	private final Direction direction;

	public BoxPlotDetector(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		if (params.contains(BoxPlotDetectorParams.OUTLIER_THRESHOLD)) {
			this.K = params.get(BoxPlotDetectorParams.OUTLIER_THRESHOLD);
		} else {
			this.K = 1.5;
		}
		this.selectedCol = params.contains(BoxPlotDetectorParams.FEATURE_COL) ? params.get(
			BoxPlotDetectorParams.FEATURE_COL)
			: dataSchema.getFieldNames()[0];
		this.direction = params.get(BoxPlotDetectorParams.DIRECTION);
	}

	@Override
	public Tuple3 <Boolean, Double, Map <String, String>>[] detect(MTable series, boolean detectLast) throws Exception {
		double[] data = OutlierUtil.getNumericArray(series, selectedCol);
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
			return new Tuple3[] {Tuple3.of(false, null, null)};
		}
		if (length <= 3) {
			Tuple3 <Boolean, Double, Map <String, String>>[] results = new Tuple3[length];
			for (int i = 0; i < length; i++) {
				results[i] = Tuple3.of(false, null, null);
			}
			return results;
		}

		double[] sortedData = new double[length];
		for (int i = 0; i < length; i++) {
			sortedData[i] = listData.get(i);
		}
		Arrays.sort(sortedData);
		final double Q1 =
			(sortedData[(int) Math.ceil(length * 0.25)] + sortedData[(int) Math.floor(length * 0.25)]) / 2;
		final double Q3 =
			(sortedData[(int) Math.ceil(length * 0.75)] + sortedData[(int) Math.floor(length * 0.75)]) / 2;
		final double IQR = Q3 - Q1;

		int iStart = detectLast ? data.length - 1 : 0;
		Tuple3 <Boolean, Double, Map <String, String>>[] results = new Tuple3[data.length - iStart];
		for (int i = iStart; i < data.length; i++) {
			double outlier_score = ((data[i] < Q1) ? (data[i] - Q1) : (data[i] > Q3) ? data[i] - Q3 : 0.0) / IQR;
			switch (direction) {
				case BOTH:
					outlier_score = Math.abs(outlier_score);
					break;
				case NEGATIVE:
					outlier_score = -outlier_score;
					break;
			}
			if (isPredDetail) {
				results[i] = Tuple3.of(outlier_score > K, outlier_score, null);
			} else {
				results[i] = Tuple3.of(outlier_score > K, null, null);
			}
		}
		return results;

	}

}
