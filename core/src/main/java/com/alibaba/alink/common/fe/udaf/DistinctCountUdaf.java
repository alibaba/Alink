package com.alibaba.alink.common.fe.udaf;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.fe.udaf.DistinctCountUdaf.DistinctCountData;
import com.alibaba.alink.common.fe.udaf.UdafUtil.DayTimeUnit;
import com.alibaba.alink.common.sql.builtin.agg.BaseUdaf;

import java.util.ArrayList;
import java.util.HashMap;

public class DistinctCountUdaf extends BaseUdaf <ArrayList <Long>, DistinctCountData> {

	final int[] windowLengths;
	final DayTimeUnit[] windowUnits;
	final int n;
	final int numFeature;
	final String[][] conditions;
	final int numCondition;

	public DistinctCountUdaf(String[] windows, int featureNum, String[][] conditions) {
		this.n = windows.length;
		this.numFeature = featureNum;
		this.conditions = conditions;
		this.numCondition = conditions == null ? 1 : conditions.length;
		Tuple2 <int[], DayTimeUnit[]> windowDetail = UdafUtil.getDayAndUnit(windows);
		this.windowLengths = windowDetail.f0;
		this.windowUnits = windowDetail.f1;
	}

	@Override
	public void accumulate(DistinctCountData sumData, Object... values) {
		int pastDays = (Integer) values[this.numFeature];
		int pastWeeks = (Integer) values[this.numFeature + 1];
		int pastMonths = (Integer) values[this.numFeature + 2];
		int pastYears = (Integer) values[this.numFeature + 3];
		Object condVal = UdafUtil.getValue(values, this.numFeature + 4);
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
				for (int i = 0; i < this.numFeature; i++) {
					if (values[i] != null) {
						for (int iw = 0; iw < windowLengths.length; iw++) {
							if (windowLengths[iw] > windowValues[iw]) {
								sumData.addData(ic, i, iw, values[i]);
							}
						}
					}
				}
			}
		}
	}

	@Override
	public void retract(DistinctCountData sumData, Object... values) {
		int pastDays = (Integer) values[this.numFeature];
		int pastWeeks = (Integer) values[this.numFeature + 1];
		int pastMonths = (Integer) values[this.numFeature + 2];
		int pastYears = (Integer) values[this.numFeature + 3];
		Object condVal = UdafUtil.getValue(values, this.numFeature + 4);
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
				for (int i = 0; i < this.numFeature; i++) {
					if (values[i] != null) {
						for (int iw = 0; iw < windowLengths.length; iw++) {
							if (windowLengths[iw] > windowValues[iw]) {
								sumData.retract(ic, i, iw, values[i]);
							}
						}
					}
				}
			}
		}
	}

	@Override
	public void resetAccumulator(DistinctCountData catCntData) {
		catCntData.reset();
	}

	@Override
	public void merge(DistinctCountData catCntData, Iterable <DistinctCountData> it) {
		for (DistinctCountData data : it) {
			catCntData.merge(data);
		}
	}

	@Override
	public ArrayList <Long> getValue(DistinctCountData accumulator) {
		ArrayList <Long> result = new ArrayList <>();
		for (int ic = 0; ic < this.numCondition; ic++) {
			for (int i = 0; i < this.numFeature; i++) {
				for (int j = 0; j < this.n; j++) {
					result.add((long) accumulator.mat[ic][i][j].size());
				}
			}
		}
		return result;
	}

	@Override
	public DistinctCountData createAccumulator() {
		return new DistinctCountData(this.numCondition, this.numFeature + 1, this.n);
	}

	public static class DistinctCountData {
		public final HashMap <Object, Integer>[][][] mat;
		public final int rows;
		public final int cols;
		public final int numCondition;

		public DistinctCountData(int numCondition, int numFeature, int numDays) {
			this.rows = numFeature;
			this.cols = numDays;
			this.numCondition = numCondition;
			this.mat = new HashMap[numCondition][rows][cols];
			for (int ic = 0; ic < numCondition; ic++) {
				for (int i = 0; i < numFeature; i++) {
					for (int j = 0; j < numDays; j++) {
						this.mat[ic][i][j] = new HashMap <Object, Integer>();
					}
				}
			}
		}

		public void addData(int conditionIndex, int rowIndex, int windowIdx, Object value) {
			if (mat[conditionIndex][rowIndex][windowIdx].containsKey(value)) {
				mat[conditionIndex][rowIndex][windowIdx].put(value,
					mat[conditionIndex][rowIndex][windowIdx].get(value) + 1);
			} else {
				mat[conditionIndex][rowIndex][windowIdx].put(value, 1);
			}
		}

		public void retract(int conditionIndex, int rowIndex, int minLevel, Object value) {
			for (int i = minLevel; i < cols; i++) {
				int count = mat[conditionIndex][rowIndex][i].get(value);
				if (count > 1) {
					mat[conditionIndex][rowIndex][i].put(value, mat[conditionIndex][rowIndex][i].get(value) - 1);
				} else {
					mat[conditionIndex][rowIndex][i].remove(value);
				}
			}
		}

		public void reset() {
			for (int ic = 0; ic < numCondition; ic++) {
				for (int i = 0; i < rows; i++) {
					for (int j = 0; j < cols; j++) {
						mat[ic][i][j].clear();
					}
				}
			}
		}

		public void merge(DistinctCountData data) {
			for (int ic = 0; ic < numCondition; ic++) {
				for (int i = 0; i < rows; i++) {
					for (int j = 0; j < cols; j++) {
						UdafUtil.mergeMap(mat[ic][i][j], data.mat[ic][i][j]);
					}
				}
			}
		}

	}

}
