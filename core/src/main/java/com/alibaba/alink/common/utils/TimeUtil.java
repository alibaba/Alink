package com.alibaba.alink.common.utils;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Preconditions;

public class TimeUtil {

	public static Time convertTime(double seconds) {
		Preconditions.checkArgument(seconds > 0, "seconds must larger than 0.");
		long milliseconds = Math.round(seconds * 1000);
		Preconditions.checkArgument(milliseconds > 0, "milliseconds must larger than 0.");
		return Time.milliseconds(milliseconds);
	}
}
