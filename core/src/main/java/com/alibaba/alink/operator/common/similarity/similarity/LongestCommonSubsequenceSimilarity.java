package com.alibaba.alink.operator.common.similarity.similarity;

import com.alibaba.alink.operator.common.similarity.Sample;

/**
 * Calculate the LCS. LCS: the longest subsequence common to the two inputs.
 */
public class LongestCommonSubsequenceSimilarity extends Similarity <Double> {
	private static final long serialVersionUID = 2422561710958769350L;
	private LongestCommonSubsequence lcs;

	public LongestCommonSubsequenceSimilarity() {
		this.lcs = new LongestCommonSubsequence();
	}

	@Override
	public double calc(Sample <Double> left, Sample <Double> right, boolean text) {
		int len = text ? Math.max(Sample.split(left.getStr()).length, Sample.split(right.getStr()).length) : Math.max(
			left.getStr().length(), right.getStr().length());
		if (len == 0) {
			return 0.0;
		}
		return lcs.calc(left, right, text) / (double) len;
	}

	/**
	 * Similarity = Distance / max(Left Length, Right Length) Override the calc function of Similarity.
	 */
	@Override
	public double calc(String left, String right) {
		int len = Math.max(left.length(), right.length());
		if (len == 0) {
			return 0.0;
		}
		return lcs.calc(left, right) / (double) len;
	}

	@Override
	public double calc(String[] left, String[] right) {
		int len = Math.max(left.length, right.length);
		if (len == 0) {
			return 0.0;
		}
		return lcs.calc(left, right) / (double) len;
	}
}
