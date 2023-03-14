package com.alibaba.alink.operator.common.statistics.statistics;

public class BooleanMeasureIterator implements BaseMeasureIterator <Boolean, BooleanMeasureIterator> {
	/**
	 * missing value number.
	 */
	public long countMissing = 0;

	/**
	 * true value number.
	 */
	public long countTrue = 0;

	/**
	 * false value number.
	 */
	public long countFalse = 0;

	public BooleanMeasureIterator() {
	}

	@Override
	public void visit(Boolean val) {
		if (null == val) {
			countMissing++;
		} else if (val) {
			countTrue++;
		} else {
			countFalse++;
		}
	}

	@Override
	public void merge(BooleanMeasureIterator iterator) {
		this.countTrue += iterator.countTrue;
		this.countFalse += iterator.countFalse;
		this.countMissing += iterator.countMissing;
	}

	@Override
	public BooleanMeasureIterator clone() {
		BooleanMeasureIterator iterator = new BooleanMeasureIterator();
		iterator.countMissing = this.countMissing;
		iterator.countTrue = this.countTrue;
		iterator.countFalse = this.countFalse;
		return iterator;
	}

	@Override
	public long missingCount() {
		return countMissing;
	}

	@Override
	public long count() {
		return countFalse + countTrue;
	}

	@Override
	public void finalResult(SummaryResultCol src) {
		long count = countTrue + countFalse;
		Boolean min = null;
		Boolean max = null;
		if (count > 0) {
			min = Boolean.FALSE;
			max = Boolean.TRUE;
			if (0 == countTrue) {
				max = Boolean.FALSE;
			}
			if (0 == countFalse) {
				min = Boolean.TRUE;
			}
		}
		src.init(null, count + countMissing, count, countMissing, 0, 0, 0, countFalse,
			countTrue, countTrue, countTrue, countTrue, countTrue, min, max);
	}

	@Override
	public String toString() {
		String result = "";
		result += String.valueOf(countTrue);
		result += " ";
		result += String.valueOf(countFalse);
		result += " ";
		result += String.valueOf(countMissing);
		return result;
	}
}
