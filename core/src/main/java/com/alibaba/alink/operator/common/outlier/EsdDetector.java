package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.probabilistic.CDF;
import com.alibaba.alink.common.probabilistic.IDF;
import com.alibaba.alink.params.outlier.HasDirection.Direction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class EsdDetector extends OutlierDetector {

	private final int inputMaxIter;
	private final double alpha;
	private final String selectedCol;
	private final Direction direction;

	public EsdDetector(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
		this.inputMaxIter = params.contains(EsdDetectorParams.MAX_ITER) ?
			params.get(EsdDetectorParams.MAX_ITER) : Integer.MAX_VALUE;
		this.selectedCol = params.contains(EsdDetectorParams.FEATURE_COL) ? params.get(EsdDetectorParams.FEATURE_COL)
			: dataSchema.getFieldNames()[0];
		this.alpha = params.get(EsdDetectorParams.ALPHA);
		this.direction = params.get(EsdDetectorParams.DIRECTION);
	}

	@Override
	public Tuple3 <Boolean, Double, Map <String, String>>[] detect(MTable series, boolean detectLast) throws Exception {
		double[] data = OutlierUtil.getNumericArray(series, selectedCol);
		int dataNum = data.length;

		int maxIter = this.inputMaxIter;
		if (null != maxOutlierRatio) {
			maxIter = Math.min(maxIter, (int) Math.round(dataNum * maxOutlierRatio));
		}
		if (null != maxOutlierNumPerGroup) {
			maxIter = Math.min(maxIter, maxOutlierNumPerGroup);
		}
		if (Integer.MAX_VALUE == maxIter) {
			maxIter = (dataNum + 9) / 10;
		}

		Tuple3 <Boolean, Double, Map <String, String>>[] results = new Tuple3[dataNum];
		for (int i = 0; i < dataNum; i++) {
			results[i] = Tuple3.of(false, Double.NEGATIVE_INFINITY, null);
		}

		HashSet <Integer> indexes = new HashSet <>(dataNum);
		for (int i = 0; i < dataNum; i++) {
			indexes.add(i);
		}

		double[] val = new double[dataNum];

		int iterNum = Math.min(dataNum, maxIter);
		double avilable_score = 1.0;
		for (int iter = 1; iter <= iterNum; iter++) {
			double mean = mean(data, indexes);
			double stdDev = stdDev(data, mean, indexes);

			switch (direction) {
				case BOTH:
					for (int i = 0; i < dataNum; i++) {
						val[i] = Math.abs(data[i] - mean);
					}
					break;
				case NEGATIVE:
					for (int i = 0; i < dataNum; i++) {
						val[i] = mean - data[i];
					}
					break;
				default:
					for (int i = 0; i < dataNum; i++) {
						val[i] = data[i] - mean;
					}
			}

			double g = Double.NEGATIVE_INFINITY;
			int gIndex = -1;
			for (Integer i : indexes) {
				if (val[i] > g) {
					g = val[i];
					gIndex = i;
				}
			}

			g = g / stdDev;

			int curNum = dataNum - iter + 1;

			if (curNum <= 2) {
				break;
			}
			double significanceLevel = Direction.BOTH == direction ? alpha / (2 * curNum) : alpha / curNum;
			double t = IDF.studentT(1 - significanceLevel, curNum - 2);
			double Gpn = (curNum - 1) / Math.sqrt(curNum * ((curNum - 2) / (t * t) + 1));

			if (g > Gpn) {
				double prob = Direction.BOTH == direction ? cdfBoth(g, curNum) : cdfSingle(g, curNum);
				HashMap <String, String> infoMap = new HashMap <>();
				infoMap.put("prob", String.valueOf(prob));
				infoMap.put("freedom_degree", String.valueOf(curNum));
				infoMap.put("iter", String.valueOf(iter));
				results[gIndex].f0 = true;
				results[gIndex].f1 = prob * avilable_score;
				results[gIndex].f2 = infoMap;
				avilable_score = results[gIndex].f1 * 0.999;
				indexes.remove(gIndex);
			} else {
				break;
			}
		}

		if (detectLast) {
			return new Tuple3[] {results[results.length - 1]};
		} else {
			return results;
		}
	}

	static double mean(double[] data, HashSet <Integer> indexes) {
		double s = 0;
		for (Integer i : indexes) {
			s += data[i];
		}
		return s / indexes.size();
	}

	static double stdDev(double[] data, double mean, HashSet <Integer> indexes) {
		double s = 0;
		for (Integer i : indexes) {
			s += (data[i] - mean) * (data[i] - mean);
		}
		return Math.sqrt(s / (indexes.size() - 1));
	}

	public static double cdfBoth(double g, int n) {
		if (g >= (n - 1) / Math.sqrt(n)) {
			return 1.0;
		} else {
			return 1 - 2 * n * (1 - CDF.studentT(Math.sqrt(n * (n - 2) / ((n - 1) * (n - 1) / (g * g) - n)),
				n - 2));
		}
	}

	public static double cdfSingle(double g, int n) {
		if (g >= (n - 1) / Math.sqrt(n)) {
			return 1.0;
		} else {
			return 1 - n * (1 - CDF.studentT(Math.sqrt(n * (n - 2) / ((n - 1) * (n - 1) / (g * g) - n)),
				n - 2));
		}
	}

}
