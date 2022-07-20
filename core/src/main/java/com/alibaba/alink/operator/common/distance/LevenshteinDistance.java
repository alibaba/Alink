package com.alibaba.alink.operator.common.distance;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.operator.common.similarity.Sample;

/**
 * Calculate the Levenshtein Distance.
 * Levenshtein metric: the minimum number of single-character edits (insertions, deletions or substitutions)
 * required to change one word into the other.
 */
public class LevenshteinDistance implements CategoricalDistance, FastCategoricalDistance <Double> {
	public static final long MAX_MEMORY = 134217728;
	private static final long serialVersionUID = -455485674940351057L;

	@Override
	public int calc(String left, String right) {
		if (left.length() == 0) {
			return right.length();
		}
		if (right.length() == 0) {
			return left.length();
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
		if (2 * lenR > MAX_MEMORY) {
			throw new AkIllegalDataException("String is Too Long for LEVENSHTEIN, please use other method."
				+ "The max length is " + MAX_MEMORY / 2);
		}

		int[][] matrix = new int[2][lenR];
		for (int j = 0; j < lenR; j++) {
			matrix[0][j] = j;
		}
		//newIndex=new line, 1-newIndex=old line
		int newIndex = 1;
		for (int i = 1; i < lenL; i++) {
			int oldIndex = 1 - newIndex;
			matrix[newIndex][0] = i;
			for (int j = 1; j < lenR; j++) {
				matrix[newIndex][j] = Math.min(matrix[oldIndex][j] + 1, matrix[newIndex][j - 1] + 1);
				matrix[newIndex][j] = Math.min(matrix[newIndex][j], matrix[oldIndex][j - 1] + (left.charAt(i - 1)
					== right.charAt(j - 1) ? 0 : 1));
			}
			newIndex = oldIndex;
		}
		return matrix[1 - newIndex][lenR - 1];
	}

	@Override
	public int calc(String[] left, String[] right) {
		if (left.length == 0) {
			return right.length;
		}
		if (right.length == 0) {
			return left.length;
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
		if (2 * lenR > MAX_MEMORY) {
			throw new AkIllegalDataException("String is Too Long for LEVENSHTEIN, please use other method."
				+ "The max length is " + MAX_MEMORY / 2);
		}
		int[][] matrix = new int[2][lenR];
		for (int j = 0; j < lenR; j++) {
			matrix[0][j] = j;
		}
		//newIndex=new line, 1-newIndex=old line
		int newIndex = 1;
		for (int i = 1; i < lenL; i++) {
			int oldIndex = 1 - newIndex;
			matrix[newIndex][0] = i;
			for (int j = 1; j < lenR; j++) {
				matrix[newIndex][j] = Math.min(matrix[oldIndex][j] + 1, matrix[newIndex][j - 1] + 1);
				matrix[newIndex][j] = Math.min(matrix[newIndex][j],
					matrix[oldIndex][j - 1] + (left[i - 1].equals(right[j - 1]) ? 0 : 1));
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
