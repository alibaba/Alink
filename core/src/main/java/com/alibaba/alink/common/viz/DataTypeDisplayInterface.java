package com.alibaba.alink.common.viz;

public interface DataTypeDisplayInterface {

	default String toDisplayData() {
		return toDisplayData(Integer.MAX_VALUE);
	}

	/**
	 * Display current object with multi lines.
	 *
	 * @param n : number of lines
	 * @return multi lines
	 */
	String toDisplayData(int n);

	/**
	 * Display current object data info.
	 *
	 * @return Data name and shape.
	 */
	String toDisplaySummary();

	/**
	 * Display current object in a short line.
	 *
	 * @return a short line
	 */
	String toShortDisplayData();

	default void print() {
		System.out.println(toDisplayData());
	}

	default void print(int n) {
		System.out.println(toDisplayData(n));
	}
}
