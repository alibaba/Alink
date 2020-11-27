package com.alibaba.alink.operator.common.similarity;

/**
 * Metric for pairwise calc.
 */
public enum Metric {
	/**
	 * <code>Levenshtein metric;</code>
	 */
	LEVENSHTEIN,
	/**
	 * <code>Levenshtein calc;</code>
	 */
	LEVENSHTEIN_SIM,
	/**
	 * <code>Lcs metric;</code>
	 */
	LCS,
	/**
	 * <code>Lcs calc;</code>
	 */
	LCS_SIM,
	/**
	 * <code>ssk calc;</code>
	 */
	SSK,
	/**
	 * <code>cosine calc;</code>
	 */
	COSINE,
	/**
	 * <code>simhash hamming metric;</code>
	 */
	SIMHASH_HAMMING,
	/**
	 * <code>simhash hamming calc;</code>
	 */
	SIMHASH_HAMMING_SIM,
	/**
	 * <code>minhash calc;</code>
	 */
	MINHASH_JACCARD_SIM,
	/**
	 * <code>Jaccard calc;</code>
	 */
	JACCARD_SIM
}
