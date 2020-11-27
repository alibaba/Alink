package com.alibaba.alink.operator.batch.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;

public class Split extends TableFunction <Tuple2 <String, Long>> {
	private static final long serialVersionUID = -3654446971802315342L;
	private String separator = " ";

	public Split(String separator) {
		this.separator = separator;
	}

	public void eval(String str, long v) {
		if (str.length() <= 0) {
			return;
		}
		for (String s : str.split(separator)) {
			// use collect(...) to emit a row
			collect(new Tuple2 <String, Long>(s, v + s.length()));
		}
	}
}
