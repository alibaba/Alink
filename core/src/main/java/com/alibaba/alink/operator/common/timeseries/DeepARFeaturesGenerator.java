package com.alibaba.alink.operator.common.timeseries;

import com.alibaba.alink.common.linalg.tensor.FloatTensor;
import com.alibaba.alink.operator.common.timeseries.TimestampUtil.TimestampToCalendar;
import com.alibaba.alink.params.timeseries.HasTimeFrequency.TimeFrequency;

import java.sql.Timestamp;

public class DeepARFeaturesGenerator {

	private static final long HOUR_TIME = 60L * 60L * 1000L;
	private static final long DAY_TIME = 24L * 60L * 60L * 1000L;
	private static final long WEEK_TIME = 7L * 24L * 60L * 60L * 1000L;
	private static final long MONTH_TIME = 30L * 7L * 24L * 60L * 60L * 1000L;

	public static TimeFrequency generateFrequency(Timestamp min, Timestamp max, long cnt) {

		long frequency = (max.getTime() - min.getTime()) / cnt;

		if (frequency < HOUR_TIME) {
			return TimeFrequency.EVERY_MINUTE;
		}

		if (frequency < DAY_TIME) {
			return TimeFrequency.HOURLY;
		}

		if (frequency < WEEK_TIME) {
			return TimeFrequency.DAILY;
		}

		if (frequency < MONTH_TIME) {
			return TimeFrequency.WEEKLY;
		}

		return TimeFrequency.MONTHLY;
	}

	public static FloatTensor generateFromFrequency(TimestampToCalendar calendar, TimeFrequency unit, Timestamp ts) {
		return generateFromFrequencyNormalize(calendar, unit, ts);
	}

	private static final float MAX_DAY_OF_WEEK = 7.f;
	private static final float MAX_DAY_OF_MONTH = 31.f;
	private static final float MAX_DAY_OF_YEAR = 365.f;
	private static final float MAX_HOUR_OF_DAY = 23.f;
	private static final float MAX_WEEK_OF_YEAR = 53.f;
	private static final float MAX_MONTH_OF_YEAR = 11.f;
	private static final float MAX_MINUTE_OF_HOUR = 59.f;

	private static FloatTensor generateFromFrequencyNormalize(
		TimestampUtil.TimestampToCalendar calendar, TimeFrequency unit, Timestamp ts) {

		switch (unit) {
			case EVERY_MINUTE:
				return new FloatTensor(new float[] {
					calendar.minuteOfHour(ts) / MAX_MINUTE_OF_HOUR,
					calendar.hourOfDay(ts) / MAX_HOUR_OF_DAY,
					calendar.dayOfWeek(ts) / MAX_DAY_OF_WEEK,
					calendar.dayOfMonth(ts) / MAX_DAY_OF_MONTH,
					calendar.dayOfYear(ts) / MAX_DAY_OF_YEAR
				});
			case HOURLY:
				return new FloatTensor(new float[] {
					calendar.hourOfDay(ts) / MAX_HOUR_OF_DAY,
					calendar.dayOfWeek(ts) / MAX_DAY_OF_WEEK,
					calendar.dayOfMonth(ts) / MAX_DAY_OF_MONTH,
					calendar.dayOfYear(ts) / MAX_DAY_OF_YEAR
				});
			case DAILY:
				return new FloatTensor(new float[] {
					calendar.dayOfWeek(ts) / MAX_DAY_OF_WEEK,
					calendar.dayOfMonth(ts) / MAX_DAY_OF_MONTH,
					calendar.dayOfYear(ts) / MAX_DAY_OF_YEAR
				});
			case WEEKLY:
				return new FloatTensor(new float[] {
					calendar.dayOfMonth(ts) / MAX_DAY_OF_MONTH,
					calendar.weekOfYear(ts) / MAX_WEEK_OF_YEAR
				});
			case MONTHLY:
				return new FloatTensor(new float[] {
					calendar.mouthOfYear(ts) / MAX_MONTH_OF_YEAR
				});
			default:
				throw new UnsupportedOperationException();
		}
	}

	private static final float MEAN_DAY_OF_WEEK = 4.0f;
	private static final float MEAN_DAY_OF_MONTH = 16.0f;
	private static final float MEAN_DAY_OF_YEAR = 183.0f;
	private static final float MEAN_HOUR_OF_DAY = 11.5f;
	private static final float MEAN_WEEK_OF_YEAR = 27.0f;
	private static final float MEAN_MONTH_OF_YEAR = 5.5f;
	private static final float MEAN_MINUTE_OF_HOUR = 29.5f;

	private static final float STD_DAY_OF_WEEK = 2.00f;
	private static final float STD_DAY_OF_MONTH = 8.94f;
	private static final float STD_DAY_OF_YEAR = 105.37f;
	private static final float STD_HOUR_OF_DAY = 6.92f;
	private static final float STD_WEEK_OF_YEAR = 15.30f;
	private static final float STD_MONTH_OF_YEAR = 3.45f;
	private static final float STD_MINUTE_OF_HOUR = 17.32f;

	private static FloatTensor generateFromFrequencyZScore(
		TimestampUtil.TimestampToCalendar calendar, TimeFrequency unit, Timestamp ts) {

		switch (unit) {
			case DAILY:
				return new FloatTensor(new float[] {
					(calendar.dayOfWeek(ts) - MEAN_DAY_OF_WEEK) / STD_DAY_OF_WEEK,
					(calendar.dayOfMonth(ts) - MEAN_DAY_OF_MONTH) / STD_DAY_OF_MONTH,
					(calendar.dayOfYear(ts) - MEAN_DAY_OF_YEAR) / STD_DAY_OF_YEAR
				});
			case HOURLY:
				return new FloatTensor(new float[] {
					(calendar.hourOfDay(ts) - MEAN_HOUR_OF_DAY) / STD_HOUR_OF_DAY,
					(calendar.dayOfWeek(ts) - MEAN_DAY_OF_WEEK) / STD_DAY_OF_WEEK,
					(calendar.dayOfMonth(ts) - MEAN_DAY_OF_MONTH) / STD_DAY_OF_MONTH,
					(calendar.dayOfYear(ts) - MEAN_DAY_OF_YEAR) / STD_DAY_OF_YEAR
				});
			case WEEKLY:
				return new FloatTensor(new float[] {
					(calendar.dayOfMonth(ts) - MEAN_DAY_OF_MONTH) / STD_DAY_OF_MONTH,
					(calendar.weekOfYear(ts) - MEAN_WEEK_OF_YEAR) / STD_WEEK_OF_YEAR
				});
			case MONTHLY:
				return new FloatTensor(new float[] {
					(calendar.mouthOfYear(ts) - MEAN_MONTH_OF_YEAR) / STD_MONTH_OF_YEAR
				});
			case EVERY_MINUTE:
				return new FloatTensor(new float[] {
					(calendar.minuteOfHour(ts) - MEAN_MINUTE_OF_HOUR) / STD_MINUTE_OF_HOUR,
					(calendar.hourOfDay(ts) - MEAN_HOUR_OF_DAY) / STD_HOUR_OF_DAY,
					(calendar.dayOfWeek(ts) - MEAN_DAY_OF_WEEK) / STD_DAY_OF_WEEK,
					(calendar.dayOfMonth(ts) - MEAN_DAY_OF_MONTH) / STD_DAY_OF_MONTH,
					(calendar.dayOfYear(ts) - MEAN_DAY_OF_YEAR) / STD_DAY_OF_YEAR
				});
			default:
				throw new UnsupportedOperationException();
		}
	}
}
