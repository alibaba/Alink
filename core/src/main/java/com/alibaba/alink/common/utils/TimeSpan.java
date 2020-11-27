/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.alibaba.alink.common.utils;

public class TimeSpan {

	private double m_TotalMilliseconds;
	private long m_days;
	private int m_hours;
	private int m_minutes;
	private int m_seconds;
	private double m_milliseconds;

	public TimeSpan(double milliseconds) {
		this.m_TotalMilliseconds = milliseconds;
		long t = (long) (m_TotalMilliseconds / 1000);
		m_milliseconds = Math.min(999.9999999999, milliseconds - t * 1000.0);
		m_seconds = (int) (t % 60);
		t /= 60;
		m_minutes = (int) (t % 60);
		t /= 60;
		m_hours = (int) (t % 24);
		m_days = t / 24;
	}

	public String __repr__() {
		return toString();
	}

	@Override
	public String toString() {
		String r = "";
		if (m_days > 0) {
			r += m_days + " days  ";
		}
		if (m_hours > 0) {
			r += m_hours + " hours  ";
		}
		if (m_minutes > 0) {
			r += m_minutes + " minutes  ";
		}
		if (m_seconds > 0) {
			r += m_seconds + " seconds  ";
		}
		return r + m_milliseconds + " milliseconds.";
	}

	public long days() {
		return m_days;
	}

	public int hours() {
		return m_hours;
	}

	public int minutes() {
		return m_minutes;
	}

	public int seconds() {
		return m_seconds;
	}

	public double milliseconds() {
		return m_milliseconds;
	}

	public double totalDays() {
		return m_TotalMilliseconds / (3600 * 24 * 1000);
	}

	public double totalHours() {
		return m_TotalMilliseconds / (3600 * 1000);
	}

	public double totalMinutes() {
		return m_TotalMilliseconds / (60 * 1000);
	}

	public double totalSeconds() {
		return m_TotalMilliseconds / 1000;
	}

	public double totalMilliseconds() {
		return m_TotalMilliseconds;
	}

}
