package com.alibaba.alink.common.fe.udaf;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.fe.udaf.KvCntUdaf.KvCntData;
import com.alibaba.alink.common.fe.udaf.UdafUtil.DayTimeUnit;
import com.alibaba.alink.common.sql.builtin.agg.BaseUdaf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class KvCntUdaf extends BaseUdaf <ArrayList <String>, KvCntData> {

	final int[] windowLengths;
	final DayTimeUnit[] windowUnits;
	final int numDays;
	final int numFeature;
	final String[][] conditions;
	final int numCondition;

	public KvCntUdaf(int numFeature, String[] windows, String[][] conditions) {
		this.numDays = windows.length;
		this.numFeature = numFeature;
		this.conditions = conditions;
		this.numCondition = conditions == null ? 1 : conditions.length;
		Tuple2 <int[], DayTimeUnit[]> windowDetail = UdafUtil.getDayAndUnit(windows);
		this.windowLengths = windowDetail.f0;
		this.windowUnits = windowDetail.f1;
	}

	@Override
	public void accumulate(KvCntData kvCntData, Object... values) {
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
				for (int iw = 0; iw < windowLengths.length; iw++) {
					if (windowLengths[iw] > windowValues[iw]) {
						kvCntData.addData(values, ic, iw);
					}
				}
			}
		}
	}

	@Override
	public void retract(KvCntData kvCntData, Object... values) {
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
				for (int iw = 0; iw < windowLengths.length; iw++) {
					if (windowLengths[iw] > windowValues[iw]) {
						kvCntData.retract(values, numCondition, iw);
					}
				}
			}
		}
	}

	@Override
	public void resetAccumulator(KvCntData kvCntData) {
		kvCntData.reset();
	}

	@Override
	public void merge(KvCntData kvCntData, Iterable <KvCntData> it) {
		for (KvCntData data : it) {
			kvCntData.merge(data);
		}
	}

	@Override
	public ArrayList <String> getValue(KvCntData accumulator) {
		ArrayList <String> result = new ArrayList <String>();
		for (int ic = 0; ic < numCondition; ic++) {
			for (int i = 0; i < this.numFeature; i++) {
				for (int j = 0; j < this.numDays; j++) {
					StringBuilder sbd = new StringBuilder();
					for (Map.Entry <Object, Integer> entry : accumulator.mat[ic][i][j].entrySet()) {
						sbd.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
					}
					if (sbd.length() == 0) {
						result.add("");
					} else {
						result.add(sbd.substring(0, sbd.length() - 1));
					}
				}
			}
		}
		return result;
	}

	@Override
	public KvCntData createAccumulator() {
		return new KvCntData(this.numCondition, this.numFeature, this.numDays);
	}

	public static class KvCntData {
		public final HashMap <Object, Integer>[][][] mat;
		public final int numDays;
		public final int numFeature;
		public final int numCondition;

		public KvCntData(int numCondition, int numFeature, int numDays) {
			this.numFeature = numFeature;
			this.numDays = numDays;
			this.numCondition = numCondition;
			this.mat = new HashMap[numCondition][numFeature][numDays];
			for (int ic = 0; ic < numCondition; ic++) {
				for (int i = 0; i < numFeature; i++) {
					for (int j = 0; j < numDays; j++) {
						mat[ic][i][j] = new HashMap <Object, Integer>();
					}
				}
			}
		}

		public void addData(Object[] objs, int conditionIndex, int minLevel) {
			for (int i = 0; i < numFeature; i++) {
				Integer k = mat[conditionIndex][i][minLevel].get(objs[i]);
				mat[conditionIndex][i][minLevel].put(objs[i], null == k ? 1 : k + 1);
			}
		}

		public void retract(Object[] objs, int conditionIndex, int minLevel) {
			for (int i = 0; i < numFeature; i++) {
				Integer k = mat[conditionIndex][i][minLevel].get(objs[i]);
				mat[conditionIndex][i][minLevel].put(objs[i], null == k ? -1 : k - 1);
			}
		}

		public void reset() {
			for (int ic = 0; ic < this.numCondition; ic++) {
				for (int i = 0; i < numFeature; i++) {
					for (int j = 0; j < numDays; j++) {
						mat[ic][i][j].clear();
					}
				}
			}
		}

		public void merge(KvCntData data) {
			for (int ic = 0; ic < this.numCondition; ic++) {
				for (int i = 0; i < numFeature; i++) {
					for (int j = 0; j < numDays; j++) {
						UdafUtil.mergeMap(mat[ic][i][j], data.mat[ic][i][j]);
					}
				}
			}
		}

	}
}
