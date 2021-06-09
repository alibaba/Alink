package com.alibaba.alink.common.sql.builtin.time;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class UnixTimeStamp extends ScalarFunction {

	private static final long serialVersionUID = -7516517393691521090L;

	@Override
	public boolean isDeterministic() {
		return false;
	}

	public long eval() {
		return System.currentTimeMillis() / 1000L;
	}

	public Long eval(String in) {
		if (in == null) {
			return null;
		}
		return Timestamp.valueOf(in).getTime() / 1000L;
	}

	public Long eval(Timestamp in) {
		if (in == null) {
			return null;
		}
		return in.getTime() / 1000L;
	}

	public Long eval(String in, String format) {
		if (in == null || format == null) {
			return null;
		}
		Date date = null;
		try {
			DateFormat sdf = new SimpleDateFormat(format);
			date = sdf.parse(in);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return date.getTime() / 1000L;
	}
}
