package com.alibaba.alink.operator.batch.graph.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class LongArrayToRow implements MapFunction <long[], Row> {
	String delimiter;
	public LongArrayToRow(String delimiter) {
		this.delimiter = delimiter;
	}

	@Override
	public Row map(long[] walk) throws Exception {
		StringBuilder s = new StringBuilder();
		for (int i = 0; i < walk.length - 1; i++) {
			s.append(walk[i]);
			s.append(delimiter);
		}
		if (walk.length >= 1) {
			s.append(walk[walk.length - 1]);
		}
		Row r = new Row(1);
		r.setField(0, s.toString());
		return r;
	}
}
