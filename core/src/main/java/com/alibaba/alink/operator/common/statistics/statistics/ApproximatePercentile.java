package com.alibaba.alink.operator.common.statistics.statistics;

import com.alibaba.alink.common.utils.AlinkSerializable;

/**
 * @author yangxu
 */
public class ApproximatePercentile implements AlinkSerializable {

	public final Class dataType;
	public ApproximateQuantile aqt = null;
	public Percentile pct = null;

	public ApproximatePercentile(Percentile pct) {
		this.pct = pct;
		dataType = pct.dataType;
	}

	public ApproximatePercentile(IntervalCalculator ic) {
		this(ic, Double.NaN, Double.NaN);
	}

	public ApproximatePercentile(IntervalCalculator ic, double min, double max) {
		this.aqt = new ApproximateQuantile(100, ic, min, max);
		this.dataType = aqt.colType;
	}

	public ApproximatePercentile(double startInterval, double stepInterval, long[] countsInterval) {
		this(startInterval, stepInterval, countsInterval, Double.NaN, Double.NaN);
	}

	public ApproximatePercentile(double startInterval, double stepInterval, long[] countsInterval, double min,
								 double max) {
		this.aqt = new ApproximateQuantile(100, startInterval, stepInterval, countsInterval, min, max);
		this.dataType = aqt.colType;
	}

	public double getPercentile(int k) {
		if (null != pct) {
			//if (dataType == Double.class) {
				return ((Number) pct.getPercentile(k)).doubleValue();
			//} else {
			//	return ((Number) pct.getPercentile(k)).longValue();
			//}
		} else {
			return aqt.getQuantile(k);
		}
	}

	public double getError(int k) {
		if (null != pct) {
			return 0;
		} else {
			return aqt.getError(k);
		}
	}

	public double getLeftBound(int k) {
		if (null != pct) {
			return getPercentile(k);
		} else {
			return aqt.getLeftBound(k);
		}
	}

	public double getRightBound(int k) {
		if (null != pct) {
			return getPercentile(k);
		} else {
			return aqt.getRightBound(k);
		}
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder();
		sbd.append("Approximate Percentile:\n");
		for (int k = 0; k <= 100; k++) {
			sbd.append(k + "%\t: " + this.getPercentile(k));
			sbd.append("\t in range [ " + this.getLeftBound(k) + " , " + this.getRightBound(k) + " )\n");
		}
		return sbd.toString();
	}

}
