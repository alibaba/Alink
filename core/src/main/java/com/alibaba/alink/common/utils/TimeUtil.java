package com.alibaba.alink.common.utils;

import org.apache.flink.streaming.api.windowing.time.Time;

import com.alibaba.alink.common.exceptions.AkPreconditions;

public class TimeUtil {

	public static Time convertTime(double seconds) {
		AkPreconditions.checkArgument(seconds > 0, "seconds must larger than 0.");
		long milliseconds = Math.round(seconds * 1000);
		AkPreconditions.checkArgument(milliseconds > 0, "milliseconds must larger than 0.");
		return Time.milliseconds(milliseconds);
	}
}
