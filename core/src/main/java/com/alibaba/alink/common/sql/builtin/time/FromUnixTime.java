package com.alibaba.alink.common.sql.builtin.time;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class FromUnixTime extends ScalarFunction {

	private static final long serialVersionUID = -1264425817731826465L;

	public String eval(Long utime) {
		if (utime == null) {
			return null;
		}
		return new Timestamp(utime * 1000L).toString();
	}

	public String eval(Long utime, String format) {
		if (utime == null || format == null) {
			return null;
		}
		try {
			DateFormat sdf = new SimpleDateFormat(format);
			return sdf.format(new Timestamp(utime * 1000L));
		} catch (Exception e) {
			return null;
		}
	}
}
