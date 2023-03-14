package com.alibaba.alink.operator.common.statistics.statistics;

public interface BaseMeasureIterator<T, I extends BaseMeasureIterator <T, I>> {
	/**
	 * add val.
	 */
	void visit(T val);

	/**
	 * merge iterator.
	 */
	void merge(I iterator);

	/**
	 * clone iterator.
	 */
	I clone();

	/**
	 * missing value number.
	 */
	long missingCount();

	/**
	 * valid value number.
	 */
	long count();

	void finalResult(SummaryResultCol src);

}
