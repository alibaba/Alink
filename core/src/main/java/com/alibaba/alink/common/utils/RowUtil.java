package com.alibaba.alink.common.utils;

import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * Utils for operations on {@link Row}.
 */
public class RowUtil {

	/*
	 * same toString function as flink 1.9
	 */
	public static String rowToString(Row row) {
		StringBuffer sb = new StringBuffer();
		if (null == row) {
			sb.append("null");
		} else {
			for (int i = 0; i < row.getArity(); i++) {
				if (i > 0) {
					sb.append(",");
				}
				final String arrayString = Arrays.deepToString(new Object[] {row.getField(i)});
				sb.append(arrayString.substring(1, arrayString.length() - 1));
			}
		}
		return sb.toString();
	}

	/**
	 * remove idx value from row.
	 */
	public static Row remove(Row row, int idx) {
		int n1 = row.getArity();
		Row ret = new Row(n1 - 1);
		for (int i = 0; i < n1; ++i) {
			if (i < idx) {
				ret.setField(i, row.getField(i));
			} else if (i > idx) {
				ret.setField(i - 1, row.getField(i));
			}
		}
		return ret;
	}

	/**
	 * merge row and objs, return a new row.
	 */
	public static Row merge(Row row, Object... objs) {
		int n1 = row.getArity();
		Row ret = new Row(n1 + objs.length);
		for (int i = 0; i < n1; ++i) {
			ret.setField(i, row.getField(i));
		}
		for (int i = 0; i < objs.length; ++i) {
			ret.setField(i + n1, objs[i]);
		}
		return ret;
	}

	/**
	 * merge obj and row, return a new row.
	 */
	public static Row merge(Object obj, Row row) {
		int n1 = row.getArity();
		Row ret = new Row(n1 + 1);
		ret.setField(0, obj);
		for (int i = 0; i < n1; ++i) {
			ret.setField(i + 1, row.getField(i));
		}
		return ret;
	}

	/**
	 * merge left and right.
	 */
	public static Row merge(Row row1, Row row2) {
		int n1 = row1.getArity();
		int n2 = row2.getArity();
		Row ret = new Row(n1 + n2);
		for (int i = 0; i < n1; ++i) {
			ret.setField(i, row1.getField(i));
		}
		for (int i = 0; i < n2; ++i) {
			ret.setField(i + n1, row2.getField(i));
		}
		return ret;
	}

}
