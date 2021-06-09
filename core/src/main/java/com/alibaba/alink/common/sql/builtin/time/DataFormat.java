package com.alibaba.alink.common.sql.builtin.time;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class DataFormat extends ScalarFunction {

	private static final long serialVersionUID = -3291007598576059037L;

	public String eval(Timestamp timestamp) {
		if (timestamp == null) {
			return null;
		}
		return timestamp.toString();
	}

	public String eval(Timestamp timestamp, String format) {
		if (timestamp == null || format == null) {
			return null;
		}
		DateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(timestamp);
	}
}
