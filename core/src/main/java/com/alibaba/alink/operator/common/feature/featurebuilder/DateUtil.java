package com.alibaba.alink.operator.common.feature.featurebuilder;

public class DateUtil {

	private static final double hundredDay = 8640000.0;
	private static final double maxSecond = 3138912000.0;//max support 100year11month.
	private static final double oneMonth = 2592000;

	public static String parseSecondTime(double second) {
		if (second < hundredDay) {
			DayTime dayTime = new DayTime(second);
			return dayTime.parseData();
		} else {
			throw new RuntimeException("Currently we only support interval less than one hundred day.");
		}
	}

	/**
	 * this is for time less than 100 days.
	 */
	public static class DayTime {
		double second = 0;
		int maxMetric = 0;
		int minute = 0;
		int hour = 0;
		int day = 0;

		public DayTime(double second) {
			buildData(second);
		}

		public void buildData(double second) {
			if (second < 100) {
				second = Math.round(second*1000000)/1000000.;
				if (Math.abs(second - 100)<1e-6) {
					this.second = 99.999999;
				} else {
					this.second = second;
				}
			} else {
				second = Math.round(second);
				this.second = second % 60;
				int minute = (int) ((second - this.second) / 60);
				if (minute == 0) {
					return;
				}
				++maxMetric;
				this.minute = minute % 60;
				int hour = (minute - this.minute) / 60;
				if (hour == 0) {
					return;
				}
				this.hour = hour % 24;
				this.day = (hour - this.hour) / 24;
			}
		}

		public String parseData() {
			if (maxMetric == 0) {
				return "\'"+second+"\' second ";
			} else {
				return "\'" + day + " " + hour + ":" + minute + ":" + (int) second + "\' day to second ";
			}
		}

		@Override
		public String toString() {
			return "day: " + day + ", hour: " + hour +
				", minute: " + minute + ", second: " + second;
		}
	}

	@Deprecated
	/*
	 * recently flink does not correctly support year and month.
	 * to use this if flink fixes its bug.
	 */
	public static class YearTime {
		int year;
		int month;
		boolean onlyMonth;

		public YearTime(double second) {
			if (second < oneMonth) {
				throw new RuntimeException("second time " + second + " should be at least one month.");
			}
			if (second > maxSecond) {
				throw new RuntimeException("second time " + second + " is too long.");
			}
			buildData(second);
		}

		void buildData(double second) {
			long month = Math.round(second / oneMonth);
			if (month < 12) {
				this.month = (int) month;
				onlyMonth = true;
			} else {
				this.month = (int) (month % 12);
				this.year = (int) ((month - this.month) / 12);
				onlyMonth = false;
			}
		}

		@Override
		public String toString() {
			return "year: " + year + ", month: " + month;
		}

		public String parseData() {
			if (onlyMonth) {
				return "\'"+month+"\' month ";
			} else {
				return "\'" + year + "-" + month + "\' year to month ";
			}
		}
	}

}
