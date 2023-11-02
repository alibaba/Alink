package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;
import com.alibaba.alink.params.outlier.HasDirection.Direction;
import com.alibaba.alink.params.outlier.KSigmaDetectorParams;

import java.util.HashMap;
import java.util.Map;

/**
 * ksigma outlier detector.
 */
public class KSigmaDetector extends OutlierDetector {

	private final double K;
	private final String selectedCol;
	private final Direction direction;

	public KSigmaDetector(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		if (params.contains(KSigmaDetectorParams.OUTLIER_THRESHOLD)) {
			this.K = params.get(KSigmaDetectorParams.OUTLIER_THRESHOLD);
		} else {
			this.K = 3.0;
		}
		this.selectedCol = params.contains(KSigmaDetectorParams.FEATURE_COL) ? params.get(
			KSigmaDetectorParams.FEATURE_COL)
			: dataSchema.getFieldNames()[0];
		this.direction = params.get(KSigmaDetectorParams.DIRECTION);
	}

	@Override
	protected Tuple3 <Boolean, Double, Map <String, String>>[] detect(MTable series, boolean detectLast)
		throws Exception {
		int n = series.getNumRow();
		double[] values = OutlierUtil.getNumericArray(series, selectedCol);
		double[] zScores;
		if (detectLast) {
			TableSummary summary = series.subSummary(new String[] {selectedCol}, 0, series.getNumRow() - 1);
			final double mean = summary.mean(selectedCol);
			final double standardDeviation = summary.standardDeviation(selectedCol);
			zScores = new double[1];
			double val = values[n - 1];
			if (standardDeviation != 0.0) {
				zScores[0] = (val - mean) / standardDeviation;
			} else {
				zScores[0] = (val != mean) && n != 1 && n != 2 ? this.K + 1 : 0;
			}
		} else {
			zScores = values;
			TableSummary summary = series.summary(selectedCol);
			final double mean = summary.mean(selectedCol);
			final double standardDeviation = summary.standardDeviation(selectedCol);

			for (int i = 0; i < zScores.length; i++) {
				if (standardDeviation != 0.0) {
					zScores[i] = (zScores[i] - mean) / standardDeviation;
				} else {
					zScores[i] = 0;
				}
			}
		}

		Tuple3 <Boolean, Double, Map <String, String>>[] results = new Tuple3[zScores.length];
		for (int i = 0; i < results.length; i++) {
			double outlier_score;
			switch (direction) {
				case BOTH:
					outlier_score = Math.abs(zScores[i]);
					break;
				case NEGATIVE:
					outlier_score = -zScores[i];
					break;
				default:
					outlier_score = zScores[i];
			}
			if (isPredDetail) {
				HashMap <String, String> infoMap = new HashMap <>();
				infoMap.put("z_score", String.valueOf(zScores[i]));
				results[i] = Tuple3.of(outlier_score > K, outlier_score, infoMap);
			} else {
				results[i] = Tuple3.of(outlier_score > K, null, null);
			}
		}
		return results;
	}

}
