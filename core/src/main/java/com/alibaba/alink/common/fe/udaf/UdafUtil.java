package com.alibaba.alink.common.fe.udaf;

import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.alink.common.exceptions.AkIllegalOperatorParameterException;

import java.time.Year;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map.Entry;

public class UdafUtil {

	static Tuple2 <int[], DayTimeUnit[]> getDayAndUnit(String[] windows) {
		if (windows == null && windows.length == 1) {
			throw new AkIllegalOperatorParameterException("window must be set. unit is d, w, m, y");
		}
		int[] windowLengths = new int[windows.length];
		DayTimeUnit[] windowUnits = new DayTimeUnit[windows.length];
		for (int i = 0; i < windows.length; i++) {
			int len = windows[i].length();
			windowLengths[i] = Integer.parseInt(windows[i].substring(0, len - 1));
			String unitStr = windows[i].substring(len - 1).toLowerCase();
			switch (unitStr) {
				case "y":
					windowUnits[i] = DayTimeUnit.YEAR;
					break;
				case "m":
					windowUnits[i] = DayTimeUnit.MONTH;
					break;
				case "w":
					windowUnits[i] = DayTimeUnit.WEEK;
					break;
				case "d":
					windowUnits[i] = DayTimeUnit.DAY;
					break;
				default:
					throw new AkIllegalOperatorParameterException("Unexpected value: " + unitStr);
			}
		}
		return Tuple2.of(windowLengths, windowUnits);
	}

	static int getMinLevel(int[] days, int n, Integer day) {
		if (day <= 0 || day > days[n - 1]) {return n;}
		for (int i = n - 2; i >= 0; i--) {
			if (day > days[i]) {return i + 1;}
		}
		return 0;
	}

	static Boolean ifSatisfyCondition(String[][] conditions, int conditionIdx, Object val) {
		if (conditions == null) {
			return true;
		}
		String[] conds = conditions[conditionIdx];
		if (conds == null || conds.length == 0) {
			return true;
		}
		if (null == val) {
			return false;
		}
		String valStr = val.toString();
		for (String cond : conds) {
			if (cond.equals(valStr)) {
				return true;
			}
		}
		return false;
	}

	static Object getValue(Object[] values, int idx) {
		return idx < values.length ? values[idx] : null;
	}

	static void mergeMap(HashMap <Object, Integer> map, HashMap <Object, Integer> map2) {
		for (Entry <Object, Integer> entry : map2.entrySet()) {
			Integer k = map.get(entry.getKey());
			map.put(entry.getKey(), entry.getValue() + (null == k ? 0 : k));
		}
	}

	public enum DayTimeUnit {
		DAY,
		WEEK,
		MONTH,
		YEAR;
	}

}
