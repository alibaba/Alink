package com.alibaba.alink.operator.common.outlier;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.TableSchema;

import com.alibaba.alink.common.MTable;
import com.alibaba.alink.common.MTableUtil;
import com.alibaba.alink.operator.common.statistics.basicstatistic.TableSummary;

import java.util.Arrays;
import java.util.List;
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
		for (String selectedCol : mt.getColNames()) {
			List <Object> values = MTableUtil.getColumn(mt, selectedCol);
			double[] data = new double[num];
			for (int i = 0; i < num; i++) {
				data[i] = ((Number) values.get(i)).doubleValue();
			}
			Arrays.sort(data);

			double m2 = summary.centralMoment2(selectedCol);
			double m3 = summary.centralMoment3(selectedCol);
			double val = m3 / Math.pow(m2, 1.5);
			boolean skewness = val < 0 ? false : true;

			for (int i = 0; i < num; i++) {

				int pos = Arrays.binarySearch(data, ((Number) values.get(i)).doubleValue());
				int s = pos;
				int t = pos;
				while (s < num - 1 && data[s] == data[pos]) {s++;}
				while (t > 0 && data[t] == data[pos]) {t--;}
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
			results[i] = Tuple3.of(scores[i] > threshold ? true : false,
				1.0 - Math.pow(2, -Math.abs(scores[i]) / threshold), null);
		}
		return results;
	}

}

