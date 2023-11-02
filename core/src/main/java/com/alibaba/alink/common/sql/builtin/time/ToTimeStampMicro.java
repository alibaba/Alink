package com.alibaba.alink.common.sql.builtin.time;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

public class ToTimeStampMicro extends ScalarFunction {
	private static final long serialVersionUID = -7665598062311579053L;

	public Timestamp eval(Long in) {
		if (in == null) {
			return null;
		}
		return new Timestamp(in);
	}
}
