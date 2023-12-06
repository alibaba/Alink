package com.alibaba.alink.common.sql.builtin.time;

import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ToTimeStampFromFormat extends ScalarFunction {
	private static final long serialVersionUID = -7665598062311579053L;

	public Timestamp eval(String in, String format) {
		if (in == null || format == null) {
			return null;
		}
		Date date = null;
		try {
			DateFormat sdf = new SimpleDateFormat(format);
			date = sdf.parse(in);
		} catch (Exception e) {
			return null;
		}
		return new Timestamp(date.getTime());
	}

}
