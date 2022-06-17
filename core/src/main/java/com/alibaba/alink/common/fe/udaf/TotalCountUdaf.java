package com.alibaba.alink.common.fe.udaf;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.fe.udaf.TotalCountUdaf.CountData;
import com.alibaba.alink.common.fe.udaf.UdafUtil.DayTimeUnit;
import com.alibaba.alink.common.sql.builtin.agg.BaseUdaf;

import java.util.ArrayList;

public class TotalCountUdaf extends BaseUdaf <ArrayList <Long>, CountData> {

	final int[] windowLengths;
	final DayTimeUnit[] windowUnits;
	final int n;
	final int m;
	final String[][] conditions;
	final int numCondition;
	final boolean isTotal;

	public TotalCountUdaf(String[] windows, int featureNum, String[][] conditions, boolean isTotal) {
		this.n = windows.length;
		this.m = featureNum;
		this.conditions = conditions;
		this.numCondition = conditions == null ? 1 : conditions.length;
		this.isTotal = isTotal;
		Tuple2 <int[], DayTimeUnit[]> windowDetail = UdafUtil.getDayAndUnit(windows);
		this.windowLengths = windowDetail.f0;
		this.windowUnits = windowDetail.f1;
	}

	@Override
	public void accumulate(CountData sumData, Object... values) {
		int pastDays = (Integer) values[this.m];
		int pastWeeks = (Integer) values[this.m + 1];
		int pastMonths = (Integer) values[this.m + 2];
		int pastYears = (Integer) values[this.m + 3];
		Object condVal = UdafUtil.getValue(values, this.m + 4);
		int[] windowValues = new int[windowLengths.length];
		for (int iw = 0; iw < windowLengths.length; iw++) {
			windowValues[iw] = pastDays;
			switch (windowUnits[iw]) {
				case WEEK:
					windowValues[iw] = pastWeeks;
					break;
				case MONTH:
					windowValues[iw] = pastMonths;
					break;
				case YEAR:
					windowValues[iw] = pastYears;
					break;
			}
		}
		for (int ic = 0; ic < numCondition; ic++) {
			if (UdafUtil.ifSatisfyCondition(this.conditions, ic, condVal)) {
				for (int i = 0; i < this.m; i++) {
					for (int iw = 0; iw < windowLengths.length; iw++) {
						if (windowLengths[iw] > windowValues[iw]) {
							if (isTotal) {
								sumData.addData(ic, i, iw);
							} else {
								if (null != values[i]) {
									sumData.addData(ic, i, iw);
								}
							}
						}
					}
				}
			}
		}
	}

	@Override
	public void retract(CountData sumData, Object... values) {
		int pastDays = (Integer) values[this.m];
		int pastWeeks = (Integer) values[this.m + 1];
		int pastMonths = (Integer) values[this.m + 2];
		int pastYears = (Integer) values[this.m + 3];
		Object condVal = UdafUtil.getValue(values, this.m + 4);
		int[] windowValues = new int[windowLengths.length];
		for (int iw = 0; iw < windowLengths.length; iw++) {
			windowValues[iw] = pastDays;
			switch (windowUnits[iw]) {
				case WEEK:
					windowValues[iw] = pastWeeks;
					break;
				case MONTH:
					windowValues[iw] = pastMonths;
					break;
				case YEAR:
					windowValues[iw] = pastYears;
					break;
			}
		}
		for (int ic = 0; ic < numCondition; ic++) {
			if (UdafUtil.ifSatisfyCondition(this.conditions, ic, condVal)) {
				for (int i = 0; i < this.m; i++) {
					for (int iw = 0; iw < windowLengths.length; iw++) {
						if (windowLengths[iw] > windowValues[iw]) {
							if (isTotal) {
								sumData.retract(ic, i, iw);
							} else {
								if (null != values[i]) {
									sumData.retract(ic, i, iw);
								}
							}
						}
					}
				}
			}
		}
	}

	@Override
	public void resetAccumulator(CountData catCntData) {
		catCntData.reset();
	}

	@Override
	public void merge(CountData catCntData, Iterable <CountData> it) {
		for (CountData data : it) {
			catCntData.merge(data);
		}
	}

	@Override
	public ArrayList <Long> getValue(CountData accumulator) {
		ArrayList <Long> result = new ArrayList <>();
		for (int ic = 0; ic < this.numCondition; ic++) {
			for (int i = 0; i < this.m; i++) {
				for (int j = 0; j < this.n; j++) {
					result.add(accumulator.mat[ic][i][j]);
				}
			}
		}
		return result;
	}

	@Override
	public CountData createAccumulator() {
		return new CountData(this.numCondition, this.m + 1, this.n);
	}

	public static class CountData {
		public final long[][][] mat;
		public final int rows;
		public final int cols;
		public final int numCondition;

		public CountData(int numCondition, int numFeature, int numDays) {
			this.rows = numFeature;
			this.cols = numDays;
			this.numCondition = numCondition;
			this.mat = new long[numCondition][rows][cols];
			reset();
		}

		public void addData(int conditionIndex, int rowIndex, int minLevel) {
			for (int i = minLevel; i < cols; i++) {
				mat[conditionIndex][rowIndex][i] += 1;
			}
		}

		public void retract(int conditionIndex, int rowIndex, int minLevel) {
			for (int i = minLevel; i < cols; i++) {
				mat[conditionIndex][rowIndex][i] -= 1;
			}
		}

		public void reset() {
			for (int ic = 0; ic < numCondition; ic++) {
				for (int i = 0; i < rows; i++) {
					for (int j = 0; j < cols; j++) {
						mat[ic][i][j] = 0;
					}
				}
			}
		}

		public void merge(CountData data) {
			for (int ic = 0; ic < numCondition; ic++) {
				for (int i = 0; i < rows; i++) {
					for (int j = 0; j < cols; j++) {
						mat[ic][i][j] += data.mat[ic][i][j];
					}
				}
			}
		}

	}

}
