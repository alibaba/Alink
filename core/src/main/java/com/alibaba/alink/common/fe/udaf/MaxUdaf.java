package com.alibaba.alink.common.fe.udaf;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.fe.udaf.MaxUdaf.MaxData;
import com.alibaba.alink.common.fe.udaf.UdafUtil.DayTimeUnit;
import com.alibaba.alink.common.sql.builtin.agg.BaseUdaf;

import java.util.ArrayList;

public class MaxUdaf extends BaseUdaf <ArrayList <Object>, MaxData> {

	final int[] windowLengths;
	final DayTimeUnit[] windowUnits;
	final int n;
	final int m;
	final String[][] conditions;
	final int numCondition;

	public MaxUdaf(String[] windows, int featureNum, String[][] conditions) {
		this.n = windows.length;
		this.m = featureNum;
		this.conditions = conditions;
		this.numCondition = conditions == null ? 1 : conditions.length;
		Tuple2 <int[], DayTimeUnit[]> windowDetail = UdafUtil.getDayAndUnit(windows);
		this.windowLengths = windowDetail.f0;
		this.windowUnits = windowDetail.f1;
	}

	@Override
	public void accumulate(MaxData sumData, Object... values) {
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
					if (values[i] != null) {
						for (int iw = 0; iw < windowLengths.length; iw++) {
							if (windowLengths[iw] > windowValues[iw]) {
								sumData.addData(ic, i, iw, (Number) values[i]);
							}
						}
					}
				}
			}
		}
	}

	@Override
	public void retract(MaxData sumData, Object... values) {
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
					if (values[i] != null) {
						for (int iw = 0; iw < windowLengths.length; iw++) {
							if (windowLengths[iw] > windowValues[iw]) {
								sumData.retract(ic, i, iw, (Number) values[i]);
							}
						}
					}
				}
			}
		}
	}

	@Override
	public void resetAccumulator(MaxData catCntData) {
		catCntData.reset();
	}

	@Override
	public void merge(MaxData catCntData, Iterable <MaxData> it) {
		for (MaxData data : it) {
			catCntData.merge(data);
		}
	}

	@Override
	public ArrayList <Object> getValue(MaxData accumulator) {
		ArrayList <Object> result = new ArrayList <>();
		for (int ic = 0; ic < this.numCondition; ic++) {
			for (int i = 0; i < this.m; i++) {
				for (int j = 0; j < this.n; j++) {
					Number num = accumulator.mat[ic][i][j];
					if (num instanceof Double && Double.NEGATIVE_INFINITY == ((Double) num)) {
						result.add(null);
					} else {
						result.add(num);
					}
				}
			}
		}
		return result;
	}

	@Override
	public MaxData createAccumulator() {
		return new MaxData(this.numCondition, this.m + 1, this.n);
	}

	public static class MaxData {
		public final Number[][][] mat;
		public final int rows;
		public final int cols;
		public final int numCondition;

		public MaxData(int numCondition, int rows, int cols) {
			this.rows = rows;
			this.cols = cols;
			this.numCondition = numCondition;
			this.mat = new Number[numCondition][rows][cols];
			reset();
		}

		public void addData(int conditionIndex, int rowIndex, int minLevel, Number val) {
			for (int i = minLevel; i < cols; i++) {
				Number matV = mat[conditionIndex][rowIndex][i];
				mat[conditionIndex][rowIndex][i] = val.doubleValue() < matV.doubleValue() ? matV : val;
			}
		}

		public void retract(int conditionIndex, int rowIndex, int minLevel, Number val) {
			throw new RuntimeException("It is not support.");
		}

		public void reset() {
			for (int ic = 0; ic < numCondition; ic++) {
				for (int i = 0; i < rows; i++) {
					for (int j = 0; j < cols; j++) {
						mat[ic][i][j] = Double.NEGATIVE_INFINITY;
					}
				}
			}
		}

		public void merge(MaxData data) {
			for (int ic = 0; ic < numCondition; ic++) {
				for (int i = 0; i < rows; i++) {
					for (int j = 0; j < cols; j++) {
						mat[ic][i][j] = data.mat[ic][i][j].doubleValue() < mat[ic][i][j].doubleValue() ?
							mat[ic][i][j] :
							data.mat[ic][i][j];
					}
				}

			}

		}
	}
}