package com.alibaba.alink.operator.common.statistics.statistics;

public class ObjectMeasureIterator<T> implements BaseMeasureIterator <T, ObjectMeasureIterator <T>> {
	/**
	 * valid value count.
	 */
	public long count = 0;

	/**
	 * missing value count.
	 */
	public long countMissing = 0;

	public ObjectMeasureIterator() {
	}


	@Override
	public long missingCount() {
		return countMissing;
	}

	@Override
	public long count() {
		return count;
	}


	@Override
	public void visit(T val) {
		if (null == val) {
			countMissing++;
		} else {
			count++;
		}
	}

	@Override
	public void merge(ObjectMeasureIterator <T> iterator) {
		this.count += iterator.count;
		this.countMissing += iterator.countMissing;
	}

	@Override
	public ObjectMeasureIterator <T> clone() {
		ObjectMeasureIterator <T> iterator = new ObjectMeasureIterator <T>();
		iterator.count = this.count;
		iterator.countMissing = this.countMissing;
		return iterator;
	}

	@Override
	public void finalResult(SummaryResultCol src) {
		src.init(null, count + countMissing, count, countMissing, 0, 0, 0, 0, 0,
			0, 0, 0, 0, null, null);
	}

	@Override
	public String toString() {
		String result = "";
		result += String.valueOf(count);
		result += " ";
		result += String.valueOf(countMissing);
		return result;
	}
}
