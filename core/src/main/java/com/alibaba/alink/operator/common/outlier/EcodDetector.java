package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;

import java.util.Arrays;
import java.util.Map;

/**
 * ECOD is similar to COPOD
 * See :cite:`Li2021ecod` (https://github.com/yzhao062/pyod/blob/master/pyod/models/ecod.py) for details.
 */
public class EcodDetector extends OutlierDetector {

	public EcodDetector(TableSchema dataSchema, Params params) {
		super(dataSchema, params);
	}

	@Override
	public Tuple3 <Boolean, Double, Map <String, String>>[] detect(MTable series, boolean detectLast) throws Exception {
		MTable mt = OutlierUtil.getMTable(series, this.params);
		String[] colNames = mt.getColNames();
		TableSummary summary = mt.summary();
		int num = mt.getNumRow();

		Tuple3 <Boolean, Double, Map <String, String>>[] results = new Tuple3[num];
		double[] scores = new double[num];
		double[] p_l = new double[num];
		double[] p_r = new double[num];
		double[] p_s = new double[num];
		Arrays.fill(p_l, 0.0);
		Arrays.fill(p_r, 0.0);
		Arrays.fill(p_s, 0.0);

		for (int colIdx = 0; colIdx < colNames.length; colIdx++) {
			String selectedCol = colNames[colIdx];
			Tuple2 <Double, Integer>[] data = new Tuple2[num];
			for (int i = 0; i < num; i++) {
				data[i] = Tuple2.of(((Number) mt.getEntry(i, colIdx)).doubleValue(), i);
			}

			Arrays.sort(data, (o1, o2) -> o1.f0.compareTo(o2.f0));
			boolean skewness = summary.skewness(selectedCol) >= 0;

			for (int i = 0; i < num; i++) {
				int pos = data[i].f1;
				int s = pos;
				int t = pos;
				while (s < num - 1 && data[s].f0 == data[pos].f0) {s++;}
				while (t > 0 && data[t].f0 == data[pos].f0) {t--;}
				double f0 = -Math.log(Math.min(1.0, (double) s / num));
				double f1 = -Math.log(Math.min(1.0, (double) (num - t) / num));

				double p = skewness ? f1 : f0;
				p_l[i] += f0;
				p_r[i] += f1;
				p_s[i] += p;
			}
		}

		double threshold;
		threshold = -Math.log(0.3) * mt.getNumCol();

		for (int i = 0; i < num; i++) {
			scores[i] = Math.max(Math.max(p_l[i], p_r[i]), p_s[i]);
			results[i] = Tuple3.of(scores[i] > threshold,
				1.0 - Math.pow(2, -Math.abs(scores[i]) / threshold), null);
		}

		return results;
	}
}

