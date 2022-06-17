package com.alibaba.alink.common.fe.udaf;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.fe.udaf.SumUdaf.SumData;
import com.alibaba.alink.common.fe.udaf.UdafUtil.DayTimeUnit;
import com.alibaba.alink.common.sql.builtin.agg.BaseUdaf;

import java.util.ArrayList;

public class SumUdaf extends BaseUdaf <ArrayList <Double>, SumData> {

	final int[] windowLengths;
	final DayTimeUnit[] windowUnits;
	final int n;
	final int m;
	final String[][] conditions;
	final int numCondition;

	public SumUdaf(String[] windows, int featureNum, String[][] conditions) {
		this.n = windows.length;
		this.m = featureNum;
		this.conditions = conditions;
		this.numCondition = conditions == null ? 1 : conditions.length;
		Tuple2 <int[], DayTimeUnit[]> windowDetail = UdafUtil.getDayAndUnit(windows);
		this.windowLengths = windowDetail.f0;
		this.windowUnits = windowDetail.f1;
	}

	@Override
	public void accumulate(SumData sumData, Object... values) {
		int pastDays = (Integer) values[this.m];
		int pastWeeks = (Integer) values[this.m + 1];
		int pastMonths = (Integer) values[this.m + 2];
		int pastYears = (Integer) values[this.m + 3];
		Object condVal = UdafUtil.getValue(values, this.m + 4);
		for (int iw = 0; iw < windowLengths.length; iw++) {
			int compareVal = pastDays;
			switch (windowUnits[iw]) {
				case WEEK:
					compareVal = pastWeeks;
					break;
				case MONTH:
					compareVal = pastMonths;
					break;
				case YEAR:
					compareVal = pastYears;
					break;
			}
			if (windowLengths[iw] > compareVal) {
				for (int ic = 0; ic < numCondition; ic++) {
					if (UdafUtil.ifSatisfyCondition(this.conditions, ic, condVal)) {
						for (int iF = 0; iF < this.m; iF++) {
							if (values[iF] != null) {
								sumData.addData(ic, iF, iw, ((Number) values[iF]).doubleValue());
							}
						}
					}
				}
			}
		}
	}

	@Override
	public void retract(SumData sumData, Object... values) {
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
								sumData.retract(ic, i, iw,
									((Number) values[i]).doubleValue());
							}
						}
					}
				}
			}
		}
	}

	@Override
	public void resetAccumulator(SumData catCntData) {
		catCntData.reset();
	}

	@Override
	public void merge(SumData catCntData, Iterable <SumData> it) {
		for (SumData data : it) {
			catCntData.merge(data);
		}
	}

	@Override
	public ArrayList <Double> getValue(SumData accumulator) {
		ArrayList <Double> result = new ArrayList <>();
		for (int ic = 0; ic < numCondition; ic++) {
			for (int i = 0; i < this.m; i++) {
				for (int j = 0; j < this.n; j++) {
					result.add(accumulator.mat[ic][i][j]);
				}
			}
		}
		return result;
	}

	@Override
	public SumData createAccumulator() {
		return new SumData(this.numCondition, this.m + 1, this.n);
	}

	public static class SumData {
		public final double[][][] mat;
		public final int rows;
		public final int cols;
		public final int numCondition;

		public SumData(int numCondition, int rows, int cols) {
			this.rows = rows;
			this.cols = cols;
			this.numCondition = numCondition;
			this.mat = new double[numCondition][rows][cols];
		}

		public void addData(int conditionIndex, int rowIndex, int windowIdx, double val) {
			mat[conditionIndex][rowIndex][windowIdx] += val;
		}

		public void retract(int conditionIndex, int rowIndex, int minLevel, double val) {
			for (int i = minLevel; i < cols; i++) {
				mat[conditionIndex][rowIndex][i] -= val;
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

		public void merge(SumData data) {
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
