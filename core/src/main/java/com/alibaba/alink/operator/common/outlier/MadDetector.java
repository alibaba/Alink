package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.params.outlier.HasDirection.Direction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MadDetector extends OutlierDetector {

	private final double D;
	private final String selectedCol;
	private final Direction direction;

	public MadDetector(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		if (params.contains(MadDetectorParams.OUTLIER_THRESHOLD)) {
			this.D = params.get(MadDetectorParams.OUTLIER_THRESHOLD);
		} else {
			this.D = 3.5;
		}
		this.selectedCol = params.contains(MadDetectorParams.FEATURE_COL) ? params.get(MadDetectorParams.FEATURE_COL)
			: dataSchema.getFieldNames()[0];
		this.direction = params.get(MadDetectorParams.DIRECTION);
	}

	@Override
	public Tuple3 <Boolean, Double, Map <String, String>>[] detect(MTable series, boolean detectLast) throws Exception {
		double[] zscores = OutlierUtil.getNumericArray(series, selectedCol);

		double[] data = zscores.clone();
		int n = data.length;
		Arrays.sort(data);
		double median = (n % 2 == 1) ? data[n >> 1] : (data[n >> 1] + data[(n >> 1) - 1]) / 2;
		for (int i = 0; i < n; i++) {
			data[i] = Math.abs(data[i] - median);
		}
		Arrays.sort(data);
		double mad = (n % 2 == 1) ? data[n >> 1] : (data[n >> 1] + data[(n >> 1) - 1]) / 2;
		double scaledMAD = mad / 0.6745;

		for (int i = 0; i < zscores.length; i++) {
			zscores[i] = (zscores[i] - median) / scaledMAD;
		}

		int iStart = detectLast ? zscores.length - 1 : 0;
		Tuple3 <Boolean, Double, Map <String, String>>[] results = new Tuple3[zscores.length - iStart];
		for (int i = iStart; i < zscores.length; i++) {
			double outlier_score;
			switch (direction) {
				case BOTH:
					outlier_score = Math.abs(zscores[i]);
					break;
				case NEGATIVE:
					outlier_score = -zscores[i];
					break;
				default:
					outlier_score= zscores[i];
			}
			if (isPredDetail) {
				HashMap <String, String> infoMap = new HashMap <>();
				infoMap.put("z_score", String.valueOf(zscores[i]));
				results[i] = Tuple3.of(outlier_score > D, outlier_score, infoMap);
			} else {
				results[i] = Tuple3.of(outlier_score > D, null, null);
			}
		}
		return results;

	}
}
