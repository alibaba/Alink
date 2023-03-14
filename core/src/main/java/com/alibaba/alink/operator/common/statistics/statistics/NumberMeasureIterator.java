package com.alibaba.alink.operator.common.statistics.statistics;

public class NumberMeasureIterator<N extends Number & Comparable <N>> implements
	BaseMeasureIterator <N, NumberMeasureIterator <N>> {
	/**
	 * valid number of data.
	 */
	public long count = 0;

	/**
	 * missing value number.
	 */
	public long countMissing = 0;

	/**
	 * nan value number.
	 */
	public long countNaN = 0;


	private long countZero4SRC = 0;
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
	public N min = null;
	/**
	 * *
	 * 最大值
	 */
	public N max = null;

	/**
	 * l1 norm.
	 */
	public double normL1 = 0.0;

	private boolean needInit = true;

	public NumberMeasureIterator() {
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
	public void visit(N val) {
		if (null == val) {
			countMissing++;
		} else if (val != val) {
			countNaN++;
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

			double d = val.doubleValue();
			sum += d;
			sum2 += d * d;
			sum3 += d * d * d;
			sum4 += d * d * d * d;
			normL1 += Math.abs(d);
			countZero4SRC += (0.0 == d) ? 1 : 0;
		}
	}

	@Override
	public void merge(NumberMeasureIterator <N> iterator) {
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
		this.countNaN += iterator.countNaN;
		this.countMissing += iterator.countMissing;
		this.normL1 += iterator.normL1;
		this.countZero4SRC += iterator.countZero4SRC;
	}

	@Override
	public NumberMeasureIterator <N> clone() {
		NumberMeasureIterator <N> iterator = new NumberMeasureIterator <N>();
		iterator.count = this.count;
		iterator.countMissing = this.countMissing;
		iterator.countNaN = this.countNaN;
		iterator.sum = this.sum;
		iterator.sum2 = this.sum2;
		iterator.sum3 = this.sum3;
		iterator.sum4 = this.sum4;
		iterator.min = this.min;
		iterator.max = this.max;
		iterator.needInit = this.needInit;
		iterator.countZero4SRC = this.countZero4SRC;
		return iterator;
	}

	@Override
	public void finalResult(SummaryResultCol src) {
		src.init(null, count + countMissing + countNaN, count,
			countMissing, countNaN, 0, 0, countZero4SRC,
			sum, normL1, sum2, sum3, sum4, min, max);
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
