/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.alibaba.alink.common.utils;

public class Stopwatch {

	private boolean bRunning;
	private long startTime;
	private long elapsedMilliseconds;

	public Stopwatch() {
		elapsedMilliseconds = 0;
		bRunning = false;
	}

	public void reset() {
		bRunning = false;
		elapsedMilliseconds = 0;
	}

	public void start() {
		bRunning = true;
		startTime = System.currentTimeMillis();
	}

	public void stop() {
		if (bRunning) {
			elapsedMilliseconds += System.currentTimeMillis() - startTime;
			bRunning = false;
		}
	}

	public boolean isRunning() {
		return bRunning;
	}

	public TimeSpan getElapsedTimeSpan() {
		return new TimeSpan((double) elapsedMilliseconds);
	}

	public double getElapsedMilliseconds() {
		return (double) elapsedMilliseconds;
	}

	public String __repr__() {
		return this.toString();
	}

	public String __str__() {
		return this.toString();
	}

	@Override
	public String toString() {
		TimeSpan ts = new TimeSpan((double) elapsedMilliseconds);
		return ts.toString();
	}
}
