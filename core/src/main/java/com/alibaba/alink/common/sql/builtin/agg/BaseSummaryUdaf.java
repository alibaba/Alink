package com.alibaba.alink.common.sql.builtin.agg;

import com.alibaba.alink.common.exceptions.AkIllegalDataException;
import com.alibaba.alink.common.sql.builtin.agg.BaseSummaryUdaf.SummaryData;

public abstract class BaseSummaryUdaf extends BaseUdaf <Object, SummaryData> {
	boolean excludeLast = false;
	boolean calc23 = true;

	public BaseSummaryUdaf() {
		this(false, true);
	}

	public BaseSummaryUdaf(boolean excludeLast) {
		this(excludeLast, true);
	}

	public BaseSummaryUdaf(boolean excludeLast, boolean calc23) {
		this.calc23 = calc23;
		this.excludeLast = excludeLast;
	}

	@Override
	public void accumulate(SummaryData acc, Object... values) {
		Object data = values[0];
		if (data == null) {
			return;
		}
		acc.addData((Number) data);
	}

	@Override
	public void retract(SummaryData acc, Object... values) {
		Object data = values[0];
		if (data == null) {
			return;
		}
		acc.retractData((Number) data);
	}

	@Override
	public void resetAccumulator(SummaryData acc) {
		acc.reset();
	}

	@Override
	public void merge(SummaryData acc, Iterable <SummaryData> it) {
		for (SummaryData data : it) {
			acc.merge(data);
		}
	}

	@Override
	public SummaryData createAccumulator() {
		return new SummaryData(excludeLast, calc23);
	}

	public static class SummaryData {
		public long count = 0;
		public double sum = 0;
		public double squareSum = 0;
		public double cubicSum = 0;

		boolean excludeLast = false;

		boolean calc23 = true;
		public Number thisData = null;
		NumberTypeHandle handle = null;

		public SummaryData() {
		}

		public SummaryData(boolean excludeLast, boolean calc23) {
			this.excludeLast = excludeLast;
			this.calc23 = calc23;
		}

		public Long getCount() {
			if (count == 0) {
				return 0L;
			}
			return count;
		}

		public Number getSum() {
			return count == 0 ? null : handle.transformData(sum);
		}

		public Number getSquareSum() {
			return count == 0 ? null : handle.transformData(squareSum);
		}

		public Number getAvg() {
			return count == 0 ? null : handle.transformData(sum / count);
		}

		public Number getVarPop() {
			if (count == 0) {
				return null;
			}

			double res = (squareSum - Math.pow(sum, 2) / count) / count;

			if (Double.isNaN(res)) {
				res = 0.0;
			}

			return handle.transformData(res);
		}

		public Number getVarSamp() {
			if (count == 0) {
				return null;
			}
			double res = getVarPop().doubleValue() * count / (count - 1);

			if (Double.isNaN(res)) {
				res = 0.0;
			}
			return handle.transformData(res);
		}

		public Number getStdPop() {
			if (count == 0) {
				return null;
			}
			double res = Math.sqrt(getVarPop().doubleValue());
			if (Double.isNaN(res)) {
				res = 0.0;
			}
			return handle.transformData(res);
		}

		public Number getStdSamp() {
			if (count == 0) {
				return null;
			}
			double res = Math.sqrt(getVarSamp().doubleValue());

			if (Double.isNaN(res)) {
				res = 0.0;
			}
			return handle.transformData(res);
		}

		public Number getSkewness() {
			if (count == 0) {
				return null;
			}
			double avg = getAvg().doubleValue();
			double std = getStdPop().doubleValue();
			double res = (cubicSum / count - avg * (3 * Math.pow(std, 2) + Math.pow(avg, 2)))
				/ Math.pow(std, 3);
			if (Double.isNaN(res)) {
				res = 0.0;
			}
			return handle.transformData(res);
		}

		public void addData(Number data) {
			if (handle == null) {
				handle = new NumberTypeHandle(data);
			}
			if (excludeLast) {
				if (thisData != null) {
					addLocalData(thisData);
				}
				thisData = data;
			} else {
				addLocalData(data);
			}
		}

		public void retractData(Number data) {
			if (count == 0) {
				if (excludeLast && thisData != null) {
					thisData = null;
				} else {
					throw new AkIllegalDataException("No data to retract.");
				}
			} else if (data != null) {
				double doubleData = data.doubleValue();
				sum -= doubleData;
				squareSum -= Math.pow(doubleData, 2);
				cubicSum -= Math.pow(doubleData, 3);
				--count;
			}
		}

		public void addLocalData(Number data) {
			if (data != null) {
				double doubleData = data.doubleValue();
				sum += doubleData;
				if (calc23) {
					squareSum += Math.pow(doubleData, 2);
					cubicSum += Math.pow(doubleData, 3);
				}
				++count;
			}
		}

		public void reset() {
			sum = 0;
			count = 0;
			squareSum = 0;
			cubicSum = 0;
			thisData = null;
		}

		public void merge(SummaryData data) {
			if (this.handle == null) {
				this.handle = data.handle;
			}
			sum += data.sum;
			count += data.count;
			squareSum += data.squareSum;
			cubicSum += data.cubicSum;
			thisData = data.thisData;
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof SummaryData)) {
				return false;
			}
			if (((SummaryData) o).excludeLast != excludeLast) {
				return false;
			}
			if (excludeLast && !((SummaryData) o).thisData.equals(thisData)) {
				return false;
			}
			if (((SummaryData) o).count != count) {
				return false;
			}
			if (((SummaryData) o).sum != sum) {
				return false;
			}
			if (((SummaryData) o).squareSum != squareSum) {
				return false;
			}
			return true;
		}

		public String toString() {
			return "count: " + this.getCount() +
				" sum: " + this.getSum() +
				" avg: " + this.getAvg();
		}

	}
}
