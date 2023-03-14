package com.alibaba.alink.operator.common.statistics.statistics;

import java.util.Date;

public class DateMeasureIterator<T extends Date> implements BaseMeasureIterator <T, DateMeasureIterator <T>> {
	/**
	 * valid value count.
	 */
	public long count = 0;

	/**
	 * missing value count.
	 */
	public long countMissing = 0;
	/**
	 * *
	 * 数据和
	 */
	public double sum = 0.0;
	/**
	 * *
	 * 数据平方和
	 */
	public double sum2 = 0.0;
	/**
	 * *
	 * 数据立方和
	 */
	public double sum3 = 0.0;
	/**
	 * *
	 * 数据四次方和
	 */
	public double sum4 = 0.0;
	/**
	 * *
	 * 最小值
	 */
	public T min = null;
	/**
	 * *
	 * 最大值
	 */
	public T max = null;

	private boolean needInit = true;

	public DateMeasureIterator() {
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

			if (needInit) {
				min = val;
				max = val;
				needInit = false;
			} else {
				if (val.compareTo(min) < 0) {
					min = val;
				}
				if (val.compareTo(max) > 0) {
					max = val;
				}
			}

			double d = (double) val.getTime();
			sum += d;
			sum2 += d * d;
			sum3 += d * d * d;
			sum4 += d * d * d * d;
		}
	}

	@Override
	public void merge(DateMeasureIterator <T> iterator) {
		if (null == this.min) {
			this.min = iterator.min;
		} else if (null != iterator.min && this.min.compareTo(iterator.min) < 0) {
			this.min = iterator.min;
		}
		if (null == this.max) {
			this.max = iterator.max;
		} else if (null != iterator.max && this.max.compareTo(iterator.max) > 0) {
			this.max = iterator.max;
		}

		this.sum += iterator.sum;
		this.sum2 += iterator.sum2;
		this.sum3 += iterator.sum3;
		this.sum4 += iterator.sum4;
		this.count += iterator.count;
		this.countMissing += iterator.countMissing;
	}

	@Override
	public DateMeasureIterator <T> clone() {
		DateMeasureIterator <T> iterator = new DateMeasureIterator <T>();
		iterator.count = this.count;
		iterator.countMissing = this.countMissing;
		iterator.sum = this.sum;
		iterator.sum2 = this.sum2;
		iterator.sum3 = this.sum3;
		iterator.sum4 = this.sum4;
		iterator.min = this.min;
		iterator.max = this.max;
		iterator.needInit = this.needInit;
		return iterator;
	}

	@Override
	public void finalResult(SummaryResultCol src) {
		src.init(null,
			count + countMissing, count, countMissing,
			0, 0, 0, 0, 0, 0, 0, 0, 0,
			min, max);
	}

	@Override
	public String toString() {
		String result = "";
		result += String.valueOf(count);
		result += " ";
		result += String.valueOf(sum);
		result += " ";
		result += String.valueOf(sum2);
		result += " ";
		result += String.valueOf(sum3);
		result += " ";
		result += String.valueOf(sum4);
		result += " ";
		result += String.valueOf(min);
		result += " ";
		result += String.valueOf(max);
		return result;
	}
}
