package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MTable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class HbosDetector extends OutlierDetector {

	private static final double DEFAULT_THRESHOLD = 30.0;
	private final static double EPS = 1e-6;

	private final int k;

	public HbosDetector(TableSchema dataSchema, Params params) {
		super(dataSchema, params);

		k = params.get(HbosDetectorParams.K);
	}

	@Override
	protected Tuple3 <Boolean, Double, Map <String, String>>[] detect(MTable series, boolean detectLast)
		throws Exception {
		series = OutlierUtil.getMTable(series, this.params);
		String[] featureCols = series.getColNames();

		int iStart = detectLast ? series.getNumRow() - 1 : 0;
		int iEnd = series.getNumRow();
		double[] histogram = new double[k];
		double[] scores = new double[iEnd - iStart];
		Arrays.fill(scores, 1.0);

		for (String featureCol : featureCols) {
			double[] values = OutlierUtil.getNumericArray(series, featureCol);

			double min = Double.MAX_VALUE, max = -Double.MAX_VALUE, sum = 0.0;
			Arrays.fill(histogram, 0.0);

			for (double val : values) {
				min = Math.min(min, val);
				max = Math.max(max, val);
				sum += val;
			}

			double step = (max - min + EPS) / k;
			double avg = 1.0 / sum;

			for (double val : values) {
				int index = (int) Math.floor((val - min) / step);
				histogram[index] += avg;
			}

			for (int j = iStart, j1 = 0; j < iEnd; ++j, ++j1) {
				int index = (int) Math.floor((values[j] - min) / step);
				scores[j1] *= histogram[index];
			}
		}

		Tuple3 <Boolean, Double, Map <String, String>>[] result = new Tuple3[scores.length];

		for (int i = 0; i < scores.length; ++i) {
			double score = -Math.log(scores[i]);

			if (isPredDetail) {
				result[i] = Tuple3.of(score > DEFAULT_THRESHOLD, score, new HashMap <>());
			} else {
				result[i] = Tuple3.of(score > DEFAULT_THRESHOLD, null, null);
			}
		}

		return result;
	}
}
