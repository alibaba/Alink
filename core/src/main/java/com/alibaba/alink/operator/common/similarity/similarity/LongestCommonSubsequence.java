package com.alibaba.alink.operator.common.similarity.similarity;

import com.alibaba.alink.operator.common.similarity.Sample;

/**
 * The Longest common subsequence algorithm returns the length of the longest subsequence that two strings have in
 * common.
 */
public class LongestCommonSubsequence extends Similarity <Double> {
	private static final long MAX_MEMORY = 134217728;
	private static final long serialVersionUID = 3340275744551590631L;

	@Override
	public double calc(String left, String right) {
		if (left.length() == 0 || right.length() == 0) {
			return 0.0;
		}
		int lenL = left.length() + 1;
		int lenR = right.length() + 1;
		//make the shorter string "right"
		if (lenL < lenR) {
			String t = right;
			right = left;
			left = t;
		}

		lenL = left.length() + 1;
		lenR = right.length() + 1;
		//memory consumption is sizeof(long) * 2 * lenR
		if (2 * (long) lenR > MAX_MEMORY) {
			throw new RuntimeException("String is Too Long for LCS, please use other method");
		}
		int[][] matrix = new int[2][lenR];
		int newIndex = 1;
		for (int i = 1; i < lenL; i++) {
			int oldIndex = 1 - newIndex;
			matrix[newIndex][0] = 0;
			for (int j = 1; j < lenR; j++) {
				if (left.charAt(i - 1) == right.charAt(j - 1)) {
					matrix[newIndex][j] = matrix[oldIndex][j - 1] + 1;
				} else {
					matrix[newIndex][j] = Math.max(matrix[newIndex][j - 1], matrix[oldIndex][j]);
				}
			}
			newIndex = oldIndex;
		}
		return matrix[1 - newIndex][lenR - 1];
	}

	@Override
	public double calc(String[] left, String[] right) {
		if (left.length == 0 || right.length == 0) {
			return 0.0;
		}
		int lenL = left.length + 1;
		int lenR = right.length + 1;
		//make the shorter string "right"
		if (lenL < lenR) {
			String[] t = right;
			right = left;
			left = t;
		}

		lenL = left.length + 1;
		lenR = right.length + 1;
		//memory consumption is sizeof(long) * 2 * lenR
		if (2 * (long) lenR > MAX_MEMORY) {
			throw new RuntimeException("String is Too Long for LCS, please use other method");
		}
		int[][] matrix = new int[2][lenR];
		int newIndex = 1;
		for (int i = 1; i < lenL; i++) {
			int oldIndex = 1 - newIndex;
			matrix[newIndex][0] = 0;
			for (int j = 1; j < lenR; j++) {
				if (left[i - 1].equals(right[j - 1])) {
					matrix[newIndex][j] = matrix[oldIndex][j - 1] + 1;
				} else {
					matrix[newIndex][j] = Math.max(matrix[newIndex][j - 1], matrix[oldIndex][j]);
				}
			}
			newIndex = oldIndex;
		}
		return matrix[1 - newIndex][lenR - 1];
	}

	@Override
	public double calc(Sample <Double> left, Sample <Double> right, boolean text) {
		return text ? calc(Sample.split(left.getStr()), Sample.split(right.getStr())) : calc(left.getStr(),
			right.getStr());
	}
}
