package com.alibaba.alink.operator.common.statistics.statistics;

import com.alibaba.alink.common.utils.AlinkSerializable;

/**
 * @author yangxu
 */
public class ApproximateQuantile implements AlinkSerializable {

	private final int numQuantile;
	public Class colType = Double.class;
	private int[] kthIndex = null;
	private double start;
	private double step;
	private double min;
	private double max;

	public ApproximateQuantile(int numQuantile, IntervalCalculator ic) {
		this(numQuantile, ic, Double.NaN, Double.NaN);
	}

	public ApproximateQuantile(int numQuantile, IntervalCalculator ic, double min, double max) {
		this(numQuantile, ic.getLeftBound().doubleValue(), ic.getStep().doubleValue(), ic.count, min, max);
	}

	public ApproximateQuantile(int numQuantile, double startInterval, double stepInterval, long[] countsInterval) {
		this(numQuantile, startInterval, stepInterval, countsInterval, Double.NaN, Double.NaN);
	}

	public ApproximateQuantile(int numQuantile, double startInterval, double stepInterval, long[] countsInterval,
							   double min, double max) {
		this.numQuantile = numQuantile;
		this.start = startInterval;
		this.step = stepInterval;
		this.min = min;
		this.max = max;
		kthIndex = new int[numQuantile + 1];
		kthIndex[0] = 0;
		long total = 0;
		for (long aCountsInterval : countsInterval) {
			total += aCountsInterval;
		}
		double s = total * 1.0 / numQuantile;
		long t1 = 0;
		long t2 = countsInterval[0];
		int k = 1;
		int idxCounts = 0;
		while (idxCounts < countsInterval.length && k <= numQuantile) {
			if (t1 <= s * k && s * k <= t2) {
				kthIndex[k] = idxCounts;
				k++;
			} else {
				t1 += countsInterval[idxCounts];
				idxCounts++;
				if (idxCounts < countsInterval.length) {
					t2 += countsInterval[idxCounts];
				} else {
					break;
				}
			}
		}
		while (k <= numQuantile) {
			kthIndex[k] = countsInterval.length - 1;
			k++;
		}
	}

	public int getNumQuantile() {
		return this.numQuantile;
	}

	public double getQuantile(int k) {
		if (0 == k && !Double.isNaN(this.min)) {
			return this.min;
		} else if (k == numQuantile && !Double.isNaN(this.max)) {
			return this.max;
		} else {
			double r = this.start + this.step * (kthIndex[k] + 0.5);
			if (!Double.isNaN(this.max)) {
				r = Math.min(this.max, r);
			}
			if (!Double.isNaN(this.min)) {
				r = Math.max(this.min, r);
			}
			return r;
		}
	}

	public double getError(int k) {
		if ((0 == k && !Double.isNaN(this.min)) || (k == numQuantile && !Double.isNaN(this.max))) {
			return 0.0;
		} else {
			return this.step / 2;
		}
	}

	public double getLeftBound(int k) {
		if (0 == k && !Double.isNaN(this.min)) {
			return this.min;
		} else if (k == numQuantile && !Double.isNaN(this.max)) {
			return this.max;
		} else {
			return this.start + this.step * kthIndex[k];
		}
	}

	public double getRightBound(int k) {
		if (0 == k && !Double.isNaN(this.min)) {
			return this.min;
		} else if (k == numQuantile && !Double.isNaN(this.max)) {
			return this.max;
		} else {
			return this.start + this.step * (kthIndex[k] + 1);
		}
	}

	public double getLowerLimit() {
		if (!Double.isNaN(this.min)) {
			return this.min;
		} else {
			return this.start;
		}
	}

	public double getUpperLimit() {
		if (!Double.isNaN(this.max)) {
			return this.max;
		} else {
			return this.start + this.step * kthIndex[numQuantile];
		}
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder();
		sbd.append("Approximate " + this.numQuantile + "-QuantileWindowFunction:\n");
		for (int k = 0; k <= numQuantile; k++) {
			sbd.append(k + "/" + this.numQuantile + "\t: " + this.getQuantile(k));
			sbd.append("\t in range [ " + this.getLeftBound(k) + " , " + this.getRightBound(k) + " )\n");
		}
		return sbd.toString();
	}
}
