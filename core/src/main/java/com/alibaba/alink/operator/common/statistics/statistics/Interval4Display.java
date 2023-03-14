package com.alibaba.alink.operator.common.statistics.statistics;

/**
 * @author yangxu
 */
public class Interval4Display {

	public int n;
	public long[] count = null;
	public int nCol = -1;
	public IntervalMeasureCalculator[][] mcs = null;
	public String[] tags = null;
	public String step = null;

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder();
		sbd.append(
			"Interval4Display{" + "n=" + n + ", count=" + count + ", nCol=" + nCol + ", mcs=" + mcs + ", tags=" + tags
				+ ", step=" + step + '}');
		for (int i = 0; i < n; i++) {
			sbd.append("\n[" + tags[i] + " ,  " + tags[i + 1] + ") : " + count[i]);
		}
		return sbd.toString();
	}
}
