/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.alibaba.alink.common.utils;

/**
 * Stopwatch象现实生活中的秒表一样，能够进行开始、停止、归零（重置）操作
 */
public class Stopwatch {

	private boolean bRunning;
	private long startTime;
	private long elapsedMilliseconds;

	public Stopwatch() {
		elapsedMilliseconds = 0;
		bRunning = false;
	}

	/**
	 * reset方法将计时归零
	 */
	public void reset() {
		bRunning = false;
		elapsedMilliseconds = 0;
	}

	/**
	 * 开始一个计时操作。
	 * 如果该对象第二次使用 start方法，将继续计时，最终的计时结果为两次计时的累加。
	 * 为避免这种情况，在第二次计时前可用reset方法将对象归零。
	 */
	public void start() {
		bRunning = true;
		startTime = System.currentTimeMillis();
	}

	/**
	 * 停止计时
	 */
	public void stop() {
		if (bRunning) {
			elapsedMilliseconds += System.currentTimeMillis() - startTime;
			bRunning = false;
		}
	}

	/**
	 * 查看一个stopwatch是否正在计时
	 *
	 * @return true or false
	 */
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
