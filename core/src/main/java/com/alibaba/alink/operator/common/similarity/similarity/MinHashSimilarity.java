package com.alibaba.alink.operator.common.similarity.similarity;

import com.alibaba.alink.operator.common.similarity.Sample;
import com.alibaba.alink.operator.common.similarity.SimilarityUtil;

import java.util.Random;

/**
 * Calculate the MinHash calc and Jaccard Similarity.
 */
public class MinHashSimilarity extends Similarity <int[][]> {
	/**
	 * the number of independent hash functions.
	 */
	private int k;

	/**
	 * number of bands.
	 */
	private int r;

	/**
	 * bands for LSH.
	 */
	private int b;
	private static final long serialVersionUID = 1791836597179954415L;
	private int[] coefficientA, coefficientB;

	/**
	 * Hash Function: h(x) = A * x + B.
	 *
	 * @param seed: seed for generate the random efficients of hash functions.
	 * @param k:    k independent hash functions.
	 * @param b:    b bands for LSH.
	 */
	public MinHashSimilarity(Long seed, int k, int b) {
		this.k = k;
		this.r = k / b;
		this.b = b;
		if (k % b != 0) {
			throw new RuntimeException("k / b != 0. k :" + k + ". b:" + b);
		}
		Random random = new Random(seed);
		coefficientA = new int[k];
		coefficientB = new int[k];
		for (int i = 0; i < k; i++) {
			coefficientA[i] = random.nextInt();
			coefficientB[i] = random.nextInt();
		}
	}

	public int[] getMinHash(int[] sorted) {
		int[] minHash = hashFuncMin(sorted);
		return minHash;
	}

	public <T> int[] getSorted(T str) {
		return SimilarityUtil.toInteger(str);
	}

	@Override
	public <M> void updateLabel(Sample sample, M str) {
		int[] sorted = getSorted(str);
		int[] hashValue = getMinHash(sorted);
		int[] buckets = toBucket(hashValue);
		sample.setStr(null);
		sample.setLabel(new int[][] {hashValue, buckets});
	}

	/**
	 * Common Function for jaccard calc and minHash calc.
	 * jaccard:
	 * True: return the jaccard calc;
	 * False: return the minHash calc.
	 */
	private <T> double similarityCommonFunc(T left, T right) {
		// Transform the String input to Integer to support hash functions.
		int[] leftSorted = SimilarityUtil.toInteger(left);
		int[] rightSorted = SimilarityUtil.toInteger(right);

		// Get k minimum values by applying hash function.
		int[] leftMinHash = hashFuncMin(leftSorted);
		int[] rightMinHash = hashFuncMin(rightSorted);

		// Compare the hash values of buckets, if all buckets are different, we think sim is 0.0.
		if (this instanceof JaccardSimilarity) {
			return crossSimilarity(leftSorted, rightSorted);
		} else {
			return crossSimilarity(leftMinHash, rightMinHash);
		}
	}

	/**
	 * Apply the hash function h(x) = A * x + B to the Vector.
	 * Get k minimum values.
	 */
	private int[] hashFuncMin(int[] wordsHash) {
		int size = wordsHash.length;
		int[] minHashSet = new int[k];
		for (int i = 0; i < k; i++) {
			minHashSet[i] = Integer.MAX_VALUE;
		}
		for (int cur : wordsHash) {
			for (int j = 0; j < k; j++) {
				int curRandWordHash = Math.abs(coefficientA[j] * cur + coefficientB[j]);
				minHashSet[j] = Math.min(minHashSet[j], curRandWordHash);
			}
		}
		return minHashSet;
	}

	/**
	 * MinHashSim = P(hmin(A) = hmin(B)) = Count(I(hmin(A) = hmin(B))) / k.
	 */
	public double crossSimilarity(int[] left, int[] right) {
		double cnt = 0;
		int size = left.length;
		for (int i = 0; i < size; i++) {
			if (left[i] == right[i]) {
				cnt += 1.0;
			}
		}
		if (size > 0) {
			cnt /= (double) size;
		}
		return cnt;
	}

	@Override
	public double calc(Sample <int[][]> left, Sample <int[][]> right, boolean text) {
		return crossSimilarity(left.getLabel()[0], right.getLabel()[0]);
	}

	/**
	 * return the hash value of one bucket.
	 */
	private int arrayHash(int[] vector, int begin, int end) {
		if (begin >= end) {
			return 0;
		}
		int ret = 1;
		for (int i = begin; i < end; i++) {
			ret = 31 * ret + vector[i];
		}
		return ret;
	}

	/**
	 * Apply LSH:
	 * For every r hash values, map them to one bucket and calculate the hash value of every bucket.
	 */
	public int[] toBucket(int[] hashValue) {
		int[] buckets = new int[b];
		int size = hashValue.length;
		if (size == 0) {
			return buckets;
		}

		int begin, end;
		int i = 0, index = 0;
		while (true) {
			if (i + r >= size) {
				begin = i;
				end = size;
				buckets[index++] = arrayHash(hashValue, begin, end);
				return buckets;
			}
			begin = i;
			end = i + r;
			buckets[index++] = arrayHash(hashValue, begin, end);
			i += r;
		}
	}

	/**
	 * Get the calc.
	 * Override the calc function of Similarity.
	 */
	@Override
	public double calc(String left, String right) {
		return similarityCommonFunc(left, right);
	}

	@Override
	public double calc(String[] left, String[] right) {
		return similarityCommonFunc(left, right);
	}

}
