package com.alibaba.alink.operator.common.similarity.similarity;

import com.alibaba.alink.operator.common.distance.LevenshteinDistance;
import com.alibaba.alink.operator.common.similarity.Sample;

/**
 * Calculate the Levenshtein Distance.
 * Levenshtein metric: the minimum number of single-character edits (insertions, deletions or substitutions)
 * required to change one word into the other.
 */

public class LevenshteinSimilarity extends Similarity <Double> {

	private static final long serialVersionUID = -6066574862546442224L;

	public LevenshteinSimilarity() {
		this.distance = new LevenshteinDistance();
	}

	/**
	 * calc = 1.0 - Normalized Distance
	 * Override the calc function of Similarity.
	 */
	@Override
	public double calc(String left, String right) {
		int len = Math.max(left.length(), right.length());
		if (len == 0) {
			return 0.0;
		}
		return 1.0 - distance.calc(left, right) / (double) len;
	}

	@Override
	public double calc(String[] left, String[] right) {
		int len = Math.max(left.length, right.length);
		if (len == 0) {
			return 0.0;
		}
		return 1.0 - distance.calc(left, right) / (double) len;
	}

	@Override
	public double calc(Sample <Double> left, Sample <Double> right, boolean text) {
		int len = text ? Math.max(Sample.split(left.getStr()).length, Sample.split(right.getStr()).length) : Math.max(
			left.getStr().length(), right.getStr().length());
		if (len == 0) {
			return 0.0;
		}
		return 1.0 - ((LevenshteinDistance) distance).calc(left, right, text) / (double) len;
	}

}
