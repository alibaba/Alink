package com.alibaba.alink.operator.common.dataproc.format;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import java.util.Map;

public class KvWriter extends FormatWriter {

	final String colDelimiter;
	final String valDelimiter;

	public KvWriter(String colDelimiter, String valDelimiter) {
		this.colDelimiter = colDelimiter;
		this.valDelimiter = valDelimiter;
	}

	@Override
	public Tuple2 <Boolean, Row> write(Map <String, String> in) {
		StringBuilder sbd = new StringBuilder();
		boolean isFirstPair = true;
		for (Map.Entry entry : in.entrySet()) {
			if (isFirstPair) {
				isFirstPair = false;
			} else {
				sbd.append(colDelimiter);
			}
			sbd.append(entry.getKey() + valDelimiter + entry.getValue());
		}

		return new Tuple2 <>(true, Row.of(sbd.toString()));
	}
}
