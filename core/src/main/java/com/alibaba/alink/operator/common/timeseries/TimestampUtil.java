package com.alibaba.alink.operator.common.timeseries;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

public class TimestampUtil implements Serializable {

	public static class TimestampToCalendar implements Serializable {
		private final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

		public int minuteOfHour(Timestamp timestamp) {
			calendar.setTime(timestamp);
			return calendar.get(Calendar.MINUTE);
		}

		public int hourOfDay(Timestamp timestamp) {
			calendar.setTime(timestamp);
			return calendar.get(Calendar.HOUR_OF_DAY);
		}

		public int dayOfWeek(Timestamp timestamp) {
			calendar.setTime(timestamp);
			return calendar.get(Calendar.DAY_OF_WEEK);
		}

		public int dayOfMonth(Timestamp timestamp) {
			calendar.setTime(timestamp);
			return calendar.get(Calendar.DAY_OF_MONTH);
		}

		public int dayOfYear(Timestamp timestamp) {
			calendar.setTime(timestamp);
			return calendar.get(Calendar.DAY_OF_YEAR);
		}

		public int weekOfYear(Timestamp timestamp) {
			calendar.setTime(timestamp);
			return calendar.get(Calendar.WEEK_OF_YEAR);
		}

		public int mouthOfYear(Timestamp timestamp) {
			calendar.setTime(timestamp);
			return calendar.get(Calendar.MONTH);
		}
	}

	private static final TimestampToCalendar CALENDAR = new TimestampToCalendar();

	public static int minuteOfHour(Timestamp timestamp) {
		return CALENDAR.minuteOfHour(timestamp);
	}

	public static int hourOfDay(Timestamp timestamp) {
		return CALENDAR.hourOfDay(timestamp);
	}

	public static int dayOfWeek(Timestamp timestamp) {
		return CALENDAR.dayOfWeek(timestamp);
	}

	public static int dayOfMonth(Timestamp timestamp) {
		return CALENDAR.dayOfMonth(timestamp);
	}

	public static int dayOfYear(Timestamp timestamp) {
		return CALENDAR.dayOfYear(timestamp);
	}

	public static int weekOfYear(Timestamp timestamp) {
		return CALENDAR.weekOfYear(timestamp);
	}

	public static int mouthOfYear(Timestamp timestamp) {
		return CALENDAR.mouthOfYear(timestamp);
	}
}
