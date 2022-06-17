package com.alibaba.alink.common.fe.udaf;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.fe.udaf.CatesCntUdaf.CatCntData;
import com.alibaba.alink.common.fe.udaf.UdafUtil.DayTimeUnit;
import com.alibaba.alink.common.sql.builtin.agg.BaseUdaf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.IntStream;

public class CatesCntUdaf extends BaseUdaf <ArrayList <Long>, CatCntData> {

	final int[] windowLengths;
	final DayTimeUnit[] windowUnits;
	final Object[] objs;
	final HashMap <Object, Integer> map;
	final int n;
	final int numFeature;
	final String[][] conditions;
	final int numCondition;

	public CatesCntUdaf(String[] objs, String[] windows, String[][] conditions) {
		this.objs = objs;
		this.n = windows.length;
		this.numFeature = objs.length;
		this.map = new HashMap <>();
		IntStream.range(0, numFeature).forEachOrdered(i -> map.put(objs[i], i));
		this.conditions = conditions;
		this.numCondition = conditions == null ? 1 : conditions.length;
		Tuple2 <int[], DayTimeUnit[]> windowDetail = UdafUtil.getDayAndUnit(windows);
		this.windowLengths = windowDetail.f0;
		this.windowUnits = windowDetail.f1;
	}

	private int getRowIndex(Object obj) {
		Integer idx = this.map.get(obj.toString());
		return null == idx ? numFeature : idx;
	}

	@Override
	public void accumulate(CatCntData catCntData, Object... values) {
		int pastDays = (Integer) values[1];
		int pastWeeks = (Integer) values[2];
		int pastMonths = (Integer) values[3];
		int pastYears = (Integer) values[4];
		Object condVal = UdafUtil.getValue(values, 5);
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
				for (int iw = 0; iw < windowLengths.length; iw++) {
					if (windowLengths[iw] > windowValues[iw]) {
						catCntData.addData(ic, getRowIndex(values[0]), iw);
					}
				}
			}
		}
	}

	@Override
	public void retract(CatCntData catCntData, Object... values) {
		int pastDays = (Integer) values[1];
		int pastWeeks = (Integer) values[2];
		int pastMonths = (Integer) values[3];
		int pastYears = (Integer) values[4];
		Object condVal = UdafUtil.getValue(values, 5);
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
				for (int iw = 0; iw < windowLengths.length; iw++) {
					if (windowLengths[iw] > windowValues[iw]) {
						catCntData.retract(ic, getRowIndex(values[0]), iw);
					}
				}
			}
		}
	}

	@Override
	public void resetAccumulator(CatCntData catCntData) {
		catCntData.reset();
	}

	@Override
	public void merge(CatCntData catCntData, Iterable <CatCntData> it) {
		for (CatCntData data : it) {
			catCntData.merge(data);
		}
	}

	@Override
	public ArrayList <Long> getValue(CatCntData accumulator) {
		ArrayList <Long> result = new ArrayList <>();
		for (int ic = 0; ic < this.numCondition; ic++) {
			for (int i = 0; i < this.numFeature; i++) {
				for (int j = 0; j < this.n; j++) {
					result.add(accumulator.mat[ic][i][j]);
				}
			}
		}
		return result;
	}

	@Override
	public CatCntData createAccumulator() {
		return new CatCntData(this.numCondition, this.numFeature + 1, this.n);
	}

	public static class CatCntData {
		public final long[][][] mat;
		public final int rows;
		public final int cols;
		public final int numCondition;

		public CatCntData(int numCondition, int rows, int cols) {
			this.rows = rows;
			this.cols = cols;
			this.numCondition = numCondition;
			this.mat = new long[numCondition][rows][cols];
		}

		public void addData(int conditionIndex, int rowIndex, int windowIdx) {
			mat[conditionIndex][rowIndex][windowIdx] += 1;
		}

		public void retract(int conditionIndex, int rowIndex, int windowIdx) {
			mat[conditionIndex][rowIndex][windowIdx] -= 1;
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

		public void merge(CatCntData data) {
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
