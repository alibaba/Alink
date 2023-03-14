package com.alibaba.alink.operator.common.statistics.statistics;

public class VarbinaryMeasureIterator implements BaseMeasureIterator <byte[], VarbinaryMeasureIterator> {
	/**
	 * valid value count.
	 */
	public long count = 0;

	/**
	 * missing value count.
	 */
	public long countMissing = 0;

	public double sum = 0.0;

	public double sum2 = 0.0;

	public double sum3 = 0.0;

	public double sum4 = 0.0;

	public int minLength = Integer.MAX_VALUE;

	public int maxLength = Integer.MIN_VALUE;

	public VarbinaryMeasureIterator() {
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
	public void visit(byte[] val) {
		if (null == val) {
			countMissing++;
		} else {
			count++;
			double d = val.length;
			sum += d;
			sum2 += d * d;
			sum3 += d * d * d;
			sum4 += d * d * d * d;
			int len = val.length;
			minLength = Math.min(minLength, len);
			maxLength = Math.max(maxLength, len);
		}
	}

	@Override
	public void merge(VarbinaryMeasureIterator iterator) {
		this.count += iterator.count;
		this.countMissing += iterator.countMissing;
		this.sum += iterator.sum;
		this.sum2 += iterator.sum2;
		this.sum3 += iterator.sum3;
		this.sum4 += iterator.sum4;
		this.minLength = Math.min(this.minLength, iterator.minLength);
		this.maxLength = Math.max(this.maxLength, iterator.maxLength);
	}

	@Override
	public VarbinaryMeasureIterator clone() {
		VarbinaryMeasureIterator iterator = new VarbinaryMeasureIterator();
		iterator.count = this.count;
		iterator.countMissing = this.countMissing;
		iterator.sum = this.sum;
		iterator.sum2 = this.sum2;
		iterator.sum3 = this.sum3;
		iterator.sum4 = this.sum4;
		iterator.minLength = this.minLength;
		iterator.maxLength = this.maxLength;
		return iterator;
	}

	@Override
	public void finalResult(SummaryResultCol src) {
		src.init(null, count + countMissing, count, countMissing, 0, 0, 0, 0,
			sum, sum, sum2, sum3, sum4,
			(count > 0) ? Integer.valueOf(minLength) : null, (count > 0) ? Integer.valueOf(maxLength) : null);
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
