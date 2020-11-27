package com.alibaba.alink.params.similarity;

/**
 * Params of StringTextPairwise.
 */
public interface StringTextPairwiseParams<T> extends
	PairwiseColsParams <T>,
	StringTextExactParams <T>,
	StringTextApproxParams <T>,
	HasMetric <T> {
}
