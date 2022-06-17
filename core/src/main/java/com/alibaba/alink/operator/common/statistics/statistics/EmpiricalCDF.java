package com.alibaba.alink.operator.common.statistics.statistics;

import java.util.ArrayList;
import java.util.Map.Entry;

public class EmpiricalCDF {

	public double[] f;
	public double[] x;
	public String colName;
	public int n;

	public EmpiricalCDF(SummaryResultCol src) throws Exception {
		if (src.hasFreq()) {
			initFreq(src.getFrequencyOrderByItem(), src.dataType);
		} else {
			initIC(src.getIntervalCalculator());
		}
		this.colName = src.colName;
	}

	private void initIC(IntervalCalculator ic) {
		double start = ic.getLeftBound().doubleValue();
		double step = ic.getStep().doubleValue();
		long[] counts = ic.getCount();

		this.n = counts.length;
		this.f = new double[n];
		this.x = new double[n];
		double s = 0.0;
		for (int i = 0; i < n; i++) {
			s += counts[i];
			f[i] = s;
			x[i] = start + step * i;
		}
		for (int i = 0; i < n; i++) {
			f[i] /= s;
		}
	}

	private void initFreq(ArrayList <Entry <Object, Long>> freq, Class dataType) throws Exception {
		n = freq.size();
		this.f = new double[n];
		this.x = new double[n];
		long count = 0;
		for (Entry <Object, Long> e : freq) {
			count += e.getValue();
		}
		double s = 0;
		for (int i = 0; i < n; i++) {
			Entry <Object, Long> e = freq.get(i);
			s += e.getValue();
			f[i] = Math.min(1.0, s / count);
			if (dataType == Double.class) {
				x[i] = ((Double) e.getKey());
			} else if (dataType == Long.class) {
				x[i] = (Long) e.getKey();
			} else if (dataType == Integer.class) {
				x[i] = (Integer) e.getKey();
			} else {
				throw new Exception("Error in datatype of column!");
			}
		}
	}
}
