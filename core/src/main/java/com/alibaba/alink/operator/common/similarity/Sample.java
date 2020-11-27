package com.alibaba.alink.operator.common.similarity;

import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * Base class to save the data for calculating distance fast.
 *
 * @param <T> the type of the pre-computed info.
 */
public class Sample<T> implements Serializable {
	private static final long serialVersionUID = 1801080642125647177L;
	/**
	 * Save the extra info besides the string.
	 */
	Row row;

	/**
	 * Save the pre-computed info of the string.
	 */
	T label;

	public void setLabel(T label) {
		this.label = label;
	}

	public void setStr(String str) {
		this.str = str;
	}

	/**
	 * Original string.
	 */
	String str;

	public String getStr() {
		return str;
	}

	public Sample(String str, Row row) {
		this(str, row, null);
	}

	public Sample(String str, Row row, T label) {
		this.row = row;
		this.label = label;
		this.str = str;
	}

	public Row getRow() {
		return row;
	}

	public T getLabel() {
		return label;
	}

	public static String[] split(String str) {
		return str.split("\\s+", -1);
	}
}
